package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
)

var cpuprofile = flag.String("cpuprofile", "", "write cpu profile to file")
var memprofile = flag.String("memprofile", "", "write memory profile to file")

type cityMap struct {
	m map[string]cityTemperatureInfo
}

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

	evaluate(flag.Args()[0])

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

	cityMap := cityMap{
		m: make(map[string]cityTemperatureInfo),
	}

	for _, l := range lines {
		processLine(l, &cityMap)
	}

	resultArray := make([]cityTemperatureResult, 0, len(cityMap.m))
	for city, info := range cityMap.m {
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
	_, _ = os.Stdout.WriteString(stringsBuilder.String()[:stringsBuilder.Len()-1])
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

func processLine(line string, cityMap *cityMap) {
	city, temperature, err := parseLine(line)
	if err != nil {
		panic(err)
	}

	temperatureFloat, err := strconv.ParseFloat(strings.TrimSpace(temperature), 64)
	if err != nil {
		panic(err)
	}

	if _, ok := cityMap.m[city]; !ok {
		cityMap.m[city] = cityTemperatureInfo{
			count: 1,
			min:   temperatureFloat,
			max:   temperatureFloat,
			sum:   temperatureFloat,
		}
	} else {
		// update city temperature info
		tempMin := cityMap.m[city].min
		tempMax := cityMap.m[city].max
		tempSum := cityMap.m[city].sum
		tempCount := cityMap.m[city].count

		if temperatureFloat < tempMin {
			tempMin = temperatureFloat
		}
		if temperatureFloat > tempMax {
			tempMax = temperatureFloat
		}
		tempSum += temperatureFloat
		tempCount++

		cityMap.m[city] = cityTemperatureInfo{
			count: tempCount,
			min:   tempMin,
			max:   tempMax,
			sum:   tempSum,
		}
	}
}

// round toward positive
func round(x float64) float64 {
	rounded := math.Round(x * 10)
	if rounded == -0.0 {
		return 0.0
	}
	return rounded / 10
}
