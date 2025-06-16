package main

import (
	"bytes"
	"errors"
	"flag"
	"hash/maphash"
	"io"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"slices"
	"strconv"
	"sync"
	"syscall"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to file")

const (
	numberOfMaxStations = 10_000
	workerCount         = 10
)

var maphashSeed = maphash.MakeSeed()

type WorkerResults [workerCount][numberOfMaxStations]cityTemperatureInfo

type cityMap [numberOfMaxStations]cityTemperatureInfo

type cityTemperatureInfo struct {
	count int64
	min   int64
	max   int64
	sum   int64
}

func main() {
	flag.Parse()
	if *cpuprofile != "" {
		f, err := os.Create("./profiles/" + *cpuprofile)
		if err != nil {
			log.Fatal("could not create CPU profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		if err := pprof.StartCPUProfile(f); err != nil {
			log.Fatal("could not start CPU profile: ", err)
		}
		defer pprof.StopCPUProfile()
	}

	evaluateMmap(flag.Args()[0], workerCount, 16*1024*1024, false)

	if *memprofile != "" {
		f, err := os.Create("./profiles/" + *memprofile)
		if err != nil {
			log.Fatal("could not create memory profile: ", err)
		}
		defer f.Close() // error handling omitted for example
		runtime.GC()    // get up-to-date statistics
		if err := pprof.WriteHeapProfile(f); err != nil {
			log.Fatal("could not write memory profile: ", err)
		}
	}
}

func evaluate(fileName string, chanSize int, chunkSize int, printResult bool) error {
	workers := runtime.NumCPU() - 1
	var (
		stationNames     = make([][]byte, 0, numberOfMaxStations)
		stationSymbolMap = make(map[uint64]uint64, numberOfMaxStations)
		workerResults    = WorkerResults{}
	)
	byChan := make(chan []byte, chanSize)

	wg := sync.WaitGroup{}
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		go func(workerID int) {
			defer wg.Done()
			for by := range byChan {
				var stationID uint64
				var startIndex int
				cityM := &workerResults[workerID]

				for i, char := range by {
					switch char {
					case ';':
						stationID = maphash.Bytes(maphashSeed, by[startIndex:i])
						startIndex = i + 1
					case '\n':
						if (i-startIndex) > 1 && stationID != 0 {
							temperature := customStringToIntParser(by[startIndex:i])
							startIndex = i + 1

							stationIndex := stationSymbolMap[stationID]

							if cityM[stationIndex].count == 0 {
								cityM[stationIndex] = cityTemperatureInfo{
									count: 1,
									min:   temperature,
									max:   temperature,
									sum:   temperature,
								}
							} else {
								cityM[stationIndex].count++
								cityM[stationIndex].sum += temperature
								if temperature < cityM[stationIndex].min {
									cityM[stationIndex].min = temperature
								}
								if temperature > cityM[stationIndex].max {
									cityM[stationIndex].max = temperature
								}
							}
						}
					}
				}
			}
		}(i)
	}

	{
		file, err := os.Open(fileName)
		if err != nil {
			panic(err)
		}
		defer file.Close()

		buf := make([]byte, chunkSize)
		leftOver := make([]byte, 0, chunkSize)

		firstIteration := true

		for {
			readTotal, err := file.Read(buf)
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				panic(err)
			}
			buf = buf[:readTotal]

			toSend := make([]byte, readTotal)
			copy(toSend, buf)

			lastNewLineIndex := bytes.LastIndex(buf, []byte{'\n'})

			toSend = append(leftOver, buf[:lastNewLineIndex+1]...)
			leftOver = make([]byte, len(buf[lastNewLineIndex+1:]))
			copy(leftOver, buf[lastNewLineIndex+1:])

			if firstIteration {
				stationNames, stationSymbolMap = getAllStationNames(toSend)
				firstIteration = false
			}

			byChan <- toSend
		}
	}
	close(byChan)
	wg.Wait()

	var cityMapResults cityMap
	for _, t := range workerResults {
		for i, tempInfo := range t {
			if cityMapResults[i].count == 0 {
				cityMapResults[i] = tempInfo
			} else {
				cityMapResults[i].count += tempInfo.count
				cityMapResults[i].sum += tempInfo.sum
				if tempInfo.min < cityMapResults[i].min {
					cityMapResults[i].min = tempInfo.min
				}
				if tempInfo.max > cityMapResults[i].max {
					cityMapResults[i].max = tempInfo.max
				}
			}
		}
	}

	slices.SortFunc(stationNames, func(a, b []byte) int {
		return bytes.Compare(a, b)
	})

	var result cityTemperatureInfo

	buf := make([]byte, 0, 50000)
	buf = append(buf, '{')

	// Print workerResults {station1=min/avg/max, station2=min/avg/max, ...}
	for i, station := range stationNames {
		if i != 0 {
			buf = append(buf, ',', ' ')
		}

		result = cityMapResults[stationSymbolMap[maphash.Bytes(maphashSeed, station)]]

		buf = append(buf, station...)
		buf = append(buf, '=')
		buf = append(buf, strconv.FormatFloat(float64(result.min)/10, 'f', 1, 64)...)
		buf = append(buf, '/')
		buf = append(buf, strconv.FormatFloat(float64(result.sum)/(float64(result.count)*10), 'f', 1, 64)...)
		buf = append(buf, '/')
		buf = append(buf, strconv.FormatFloat(float64(result.max)/10, 'f', 1, 64)...)
	}

	buf = append(buf, '}', '\n')
	if printResult {
		_, _ = os.Stdout.Write(buf)
	}
	return nil
}

func getAllStationNames(by []byte) ([][]byte, map[uint64]uint64) {
	stationNames := make([][]byte, 0, numberOfMaxStations)
	stationSymbolMap := make(map[uint64]uint64, numberOfMaxStations)
	var startIndex int

	var id uint64
	for i, char := range by {
		switch char {
		case ';':
			stationID := maphash.Bytes(maphashSeed, by[startIndex:i])
			if _, ok := stationSymbolMap[stationID]; !ok {
				stationNames = append(stationNames, by[startIndex:i])
				stationSymbolMap[stationID] = id
				id++
			}
		case '\n':
			startIndex = i + 1
		}
	}

	return stationNames, stationSymbolMap
}

// input: string containing signed number in the range [-99.9, 99.9]
// output: signed int in the range [-999, 999]
func customStringToIntParser(input []byte) (output int64) {
	var isNegativeNumber bool
	if input[0] == '-' {
		isNegativeNumber = true
		input = input[1:]
	}

	switch len(input) {
	case 3:
		// 1.2 -> 12
		output = int64(input[0])*10 + int64(input[2]) - '0'*11
	case 4:
		// 11.2 -> 112
		output = int64(input[0])*100 + int64(input[1])*10 + int64(input[3]) - '0'*111
	}

	if isNegativeNumber {
		return -output
	}
	return
}

func evaluateMmap(fileName string, _ int, _ int, printResult bool) error {
	var (
		workerResults    = WorkerResults{}
		stationNames     = make([][]byte, 0, numberOfMaxStations)
		stationResults   = [numberOfMaxStations]cityTemperatureInfo{}
		stationSymbolMap = make(map[uint64]uint64, numberOfMaxStations)
	)

	f, err := os.Open(fileName)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	stat, _ := f.Stat()
	size := stat.Size()

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(data)

	var (
		id        uint64
		pos       int
		off       int
		stationID uint64
	)

	// get all station names, assume all station are in the first 5_000_000 lines
	for pos <= 5_000_000 {
		for j, c := range data[pos:] {
			if c == ';' {
				off = j
				break
			}
		}

		stationID = maphash.Bytes(maphashSeed, data[pos:pos+off])
		if _, ok := stationSymbolMap[stationID]; !ok {
			stationNames = append(stationNames, data[pos:pos+off])
			stationSymbolMap[stationID] = id
			id++
		}

		pos += off + 2

		if data[pos+2] == '.' {
			// -21.3\n
			pos += 5
		} else if data[pos+1] == '.' {
			// 21.3\n or -1.3\n
			pos += 4
		} else if data[pos] == '.' {
			// 1.3\n
			pos += 3
		}
	}

	workerSize := len(data) / workerCount

	done := make(chan struct{}, workerCount)

	go func() {
		// sort station names
		slices.SortFunc(stationNames, func(a, b []byte) int {
			return bytes.Compare(a, b)
		})

		done <- struct{}{}
	}()

	for workerID := 0; workerID < workerCount; workerID++ {
		// process data in parallel
		go func(workerID int, data []byte) {
			last := workerSize*(workerID+1) + 20
			if last > len(data) {
				last = len(data) - 1
			}

			data = data[workerSize*workerID : last]
			data = data[bytes.IndexByte(data, '\n')+1 : bytes.LastIndexByte(data, '\n')+1]

			var (
				pos         int
				off         int
				stationID   uint64
				temperature int64
			)

			for {
				// find semicolon to get station name
				off = -1

				for j, c := range data[pos:] {
					if c == ';' {
						off = j
						break
					}
				}

				if off == -1 {
					break
				}

				// translate station name to station ID
				stationID = stationSymbolMap[maphash.Bytes(maphashSeed, data[pos:pos+off])]
				pos += off + 1

				// parse temperature
				{
					negative := data[pos] == '-'
					if negative {
						pos++
					}

					if data[pos+1] == '.' {
						// 1.2\n
						temperature = int64(data[pos+2]) + int64(data[pos+0])*10 - '0'*(11)
						pos += 4
					} else {
						// 12.3\n
						temperature = int64(data[pos+3]) + int64(data[pos+1])*10 + int64(data[pos+0])*100 - '0'*(111)
						pos += 5
					}

					if negative {
						temperature = -temperature
					}
				}

				workerResults[workerID][stationID].count++
				workerResults[workerID][stationID].sum += temperature
				if temperature < workerResults[workerID][stationID].min {
					workerResults[workerID][stationID].min = temperature
				}
				if temperature > workerResults[workerID][stationID].max {
					workerResults[workerID][stationID].max = temperature
				}
			}

			done <- struct{}{}
		}(workerID, data)
	}

	// wait for all workers to finish
	for i := 0; i <= workerCount; i++ {
		<-done
	}

	// merge workerResults
	for _, result := range workerResults {
		for stationID, stationResult := range result {
			if stationResult.count == 0 {
				continue
			}

			stationResults[stationID].sum += stationResult.sum
			stationResults[stationID].count += stationResult.count
			if stationResult.min < stationResults[stationID].min {
				stationResults[stationID].min = stationResult.min
			}
			if stationResult.max > stationResults[stationID].max {
				stationResults[stationID].max = stationResult.max
			}
		}
	}

	var result cityTemperatureInfo

	buf := make([]byte, 0, 50000)
	buf = append(buf, '{')

	// Print workerResults {station1=min/avg/max, station2=min/avg/max, ...}
	for i, station := range stationNames {
		if i != 0 {
			buf = append(buf, ',', ' ')
		}

		result = stationResults[stationSymbolMap[maphash.Bytes(maphashSeed, station)]]

		buf = append(buf, station...)
		buf = append(buf, '=')
		buf = append(buf, strconv.FormatFloat(float64(result.min)/10, 'f', 1, 64)...)
		buf = append(buf, '/')
		buf = append(buf, strconv.FormatFloat(float64(result.sum)/(float64(result.count)*10), 'f', 1, 64)...)
		buf = append(buf, '/')
		buf = append(buf, strconv.FormatFloat(float64(result.max)/10, 'f', 1, 64)...)
	}

	buf = append(buf, '}', '\n')
	if printResult {
		_, _ = os.Stdout.Write(buf)
	}
	return nil
}
