package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to file")

type cityMap map[string]cityTemperatureInfo

type cityTemperatureInfo struct {
	count int64
	min   float64
	max   float64
	sum   float64
}

type cityTemperatureResult struct {
	city          string
	min, max, avg float64
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

	evaluate(flag.Args()[0], 10, 16*1024*1024, false)

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
	byChan := make(chan []byte, chanSize)
	resultChan := make(chan cityMap, chanSize)

	workers := runtime.NumCPU() - 1
	wg := sync.WaitGroup{}
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			for by := range byChan {
				processBytes(by, resultChan)
			}
		}()
	}

	go func() {
		if err := readFile(fileName, chunkSize, byChan); err != nil {
			log.Fatal("could not read file: ", err)
		}
		close(byChan)
		wg.Wait()
		close(resultChan)
	}()

	cityMap := make(cityMap)
	for t := range resultChan {
		for city, tempInfo := range t {
			if val, ok := cityMap[city]; ok {
				val.count += tempInfo.count
				val.sum += tempInfo.sum
				if tempInfo.min < val.min {
					val.min = tempInfo.min
				}

				if tempInfo.max > val.max {
					val.max = tempInfo.max
				}
				cityMap[city] = val
			} else {
				cityMap[city] = tempInfo
			}
		}
	}

	resultArray := make([]cityTemperatureResult, 0, len(cityMap))
	for city, info := range cityMap {
		resultArray = append(resultArray, cityTemperatureResult{
			city: city,
			min:  round(info.min / 10),
			max:  round(info.max / 10),
			avg:  round(info.sum / float64(info.count) / 10),
		})
	}

	sort.Slice(resultArray, func(i, j int) bool {
		return resultArray[i].city < resultArray[j].city
	})

	var stringsBuilder strings.Builder
	for _, i := range resultArray {
		stringsBuilder.WriteString(fmt.Sprintf("%s=%.1f/%.1f/%.1f; ", i.city, i.min, i.avg, i.max))
	}
	if printResult {
		_, _ = os.Stdout.WriteString(stringsBuilder.String()[:stringsBuilder.Len()-1])
	}
	return nil
}

func readFile(fileName string, chunkSize int, byChan chan []byte) error {
	file, err := os.Open(fileName)
	if err != nil {
		return err
	}
	defer file.Close()

	buf := make([]byte, chunkSize)
	leftOver := make([]byte, 0, chunkSize)

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

		byChan <- toSend
	}

	return nil
}

func processBytes(by []byte, resultChan chan<- cityMap) {
	cityMap := make(cityMap)
	var city string
	var startIndex int

	stringBuf := string(by)

	for i, char := range stringBuf {
		switch char {
		case ';':
			city = stringBuf[startIndex:i]
			startIndex = i + 1
		case '\n':
			if (i-startIndex) > 1 && len(city) != 0 {
				temperature, err := strconv.ParseFloat(strings.TrimSpace(stringBuf[startIndex:i]), 64)
				if err != nil {
					panic(err)
				}
				startIndex = i + 1
				if val, ok := cityMap[city]; !ok {
					cityMap[city] = cityTemperatureInfo{
						count: 1,
						min:   temperature,
						max:   temperature,
						sum:   temperature,
					}
				} else {
					val.count++
					val.sum += temperature
					if temperature < val.min {
						val.min = temperature
					}
					if temperature > val.max {
						val.max = temperature
					}
					cityMap[city] = val
				}

				city = ""
			}
		}
	}
	resultChan <- cityMap
}

// round toward positive
func round(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == 0.0 {
		return 0.0
	}
	return rounded / 10
}
