package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
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

	fmt.Println(evaluate(flag.Args()[0]))

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

func evaluate(fileName string) error {
	lines, err := readFileLines(fileName)
	if err != nil {
		return err
	}

	cityMap := make(cityMap)

	for _, line := range lines {
		city, temperature, err := parseLine(line)
		if err != nil {
			return err
		}

		temperatureFloat, err := strconv.ParseFloat(strings.TrimSpace(temperature), 64)
		if err != nil {
			return err
		}

		if _, ok := cityMap[city]; !ok {
			cityMap[city] = cityTemperatureInfo{
				count: 1,
				min:   temperatureFloat,
				max:   temperatureFloat,
				sum:   temperatureFloat,
			}
		} else {
			// update city temperature info
			tempMin := cityMap[city].min
			tempMax := cityMap[city].max
			tempSum := cityMap[city].sum
			tempCount := cityMap[city].count

			if temperatureFloat < tempMin {
				tempMin = temperatureFloat
			}
			if temperatureFloat > tempMax {
				tempMax = temperatureFloat
			}
			tempSum += temperatureFloat
			tempCount++

			cityMap[city] = cityTemperatureInfo{
				count: tempCount,
				min:   tempMin,
				max:   tempMax,
				sum:   tempSum,
			}
		}
	}

	for city, info := range cityMap {
		fmt.Printf("%s: %d, %f, %f, %f\n", city, info.count, info.min, info.max, info.sum)
	}

	return nil
}

func readFileLines(fileName string) ([]string, error) {
	file, err := os.Open(fileName)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	lines := make([]string, 0)
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	return lines, nil
}

func parseLine(line string) (string, string, error) {
	parts := strings.Split(line, ";")
	if len(parts) != 2 {
		return "", "", fmt.Errorf("invalid line: %s", line)
	}

	city := parts[0]
	temperature := parts[1]

	return city, temperature, nil
}
