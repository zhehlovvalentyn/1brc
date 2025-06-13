package main

import (
	"fmt"
	"testing"
)

func BenchmarkChanSize(b *testing.B) {
	testCases := []struct {
		chanSize  int
		chunkSize int
		fileName  string
	}{
		{10, 10000, "data/measurements_100m.txt"},
		{100, 10000, "data/measurements_100m.txt"},
		{1000, 10000, "data/measurements_100m.txt"},
	}

	for _, testCase := range testCases {
		b.Run(fmt.Sprintf("chanSize=%d,chunkSize=%d", testCase.chanSize, testCase.chunkSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				evaluate(testCase.fileName, testCase.chanSize, testCase.chunkSize, false)
			}
		})
	}
}

func BenchmarkChunkSize(b *testing.B) {
	testCases := []struct {
		chanSize  int
		chunkSize int
		fileName  string
	}{
		{10, 64 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 32 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 16 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 8 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 4 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 2 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 1 * 1024 * 1024, "data/measurements_100m.txt"},
		{10, 512 * 1024, "data/measurements_100m.txt"},
		{10, 256 * 1024, "data/measurements_100m.txt"},
	}

	for _, testCase := range testCases {
		b.Run(fmt.Sprintf("chanSize=%d,chunkSize=%d", testCase.chanSize, testCase.chunkSize), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				evaluate(testCase.fileName, testCase.chanSize, testCase.chunkSize, false)
			}
		})
	}
}
