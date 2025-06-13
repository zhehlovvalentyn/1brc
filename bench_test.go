package main

import "testing"

func BenchmarkEvaluate(b *testing.B) {
	for i := 0; i < b.N; i++ {
		evaluate("data/measurements_100m.txt")
	}
}