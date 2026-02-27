package main

import (
	"fmt"
	"time"

	rsema1d "github.com/celestiaorg/rsema1d"
)

func makeTestData(k, rowSize int) [][]byte {
	data := make([][]byte, k)
	for i := 0; i < k; i++ {
		data[i] = make([]byte, rowSize)
		// Fill with pattern
		for j := 0; j < rowSize; j++ {
			data[i][j] = byte((i + j) % 256)
		}
	}
	return data
}

func benchmarkEncode(name string, k, n, rowSize int) {
	data := makeTestData(k, rowSize)
	config := &rsema1d.Config{
		K:           k,
		N:           n,
		RowSize:     rowSize,
		WorkerCount: 1,
	}

	// Warmup
	for i := 0; i < 10; i++ {
		rsema1d.Encode(data, config)
	}

	// Measure
	iterations := 100
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, _, _, err := rsema1d.Encode(data, config)
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)
	avgTime := elapsed / time.Duration(iterations)

	totalBytes := k * rowSize
	throughputMBs := float64(totalBytes) / avgTime.Seconds() / 1024 / 1024

	fmt.Printf("Encode_%s: %.4fms, %.2f MiB/s\n", name, float64(avgTime.Microseconds())/1000.0, throughputMBs)
}

func benchmarkProofGen(name string, k, n, rowSize int) {
	data := makeTestData(k, rowSize)
	config := &rsema1d.Config{
		K:           k,
		N:           n,
		RowSize:     rowSize,
		WorkerCount: 1,
	}

	extData, _, _, err := rsema1d.Encode(data, config)
	if err != nil {
		panic(err)
	}

	// Warmup
	for i := 0; i < 100; i++ {
		extData.GenerateRowProof(0)
	}

	// Measure
	iterations := 10000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		_, err := extData.GenerateRowProof(0)
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)
	avgTime := elapsed / time.Duration(iterations)

	fmt.Printf("ProofGen_%s: %.4fms\n", name, float64(avgTime.Nanoseconds())/1000000.0)
}

func benchmarkVerification(name string, k, n, rowSize int) {
	data := makeTestData(k, rowSize)
	config := &rsema1d.Config{
		K:           k,
		N:           n,
		RowSize:     rowSize,
		WorkerCount: 1,
	}

	extData, commitment, _, err := rsema1d.Encode(data, config)
	if err != nil {
		panic(err)
	}

	proof, err := extData.GenerateStandaloneProof(0)
	if err != nil {
		panic(err)
	}

	// Warmup
	for i := 0; i < 100; i++ {
		rsema1d.VerifyStandaloneProof(proof, commitment, config)
	}

	// Measure
	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		err := rsema1d.VerifyStandaloneProof(proof, commitment, config)
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)
	avgTime := elapsed / time.Duration(iterations)

	fmt.Printf("Verification_%s: %.4fms\n", name, float64(avgTime.Microseconds())/1000.0)
}

func main() {
	fmt.Println("# Go Benchmark Results")

	// Encode benchmarks - matching Rust configurations
	benchmarkEncode("128KB_k1024_n1024", 1024, 1024, 128)
	benchmarkEncode("1MB_k1024_n1024", 1024, 1024, 1024)
	benchmarkEncode("1MB_k4096_n4096", 4096, 4096, 256)
	benchmarkEncode("8MB_k4096_n4096", 4096, 4096, 2048)
	benchmarkEncode("128MB_k4096_n4096", 4096, 4096, 32768)
	benchmarkEncode("128MB_k8192_n8192", 8192, 8192, 16384)

	// Proof generation benchmarks
	benchmarkProofGen("1MB_k1024", 1024, 1024, 1024)
	benchmarkProofGen("8MB_k4096", 4096, 4096, 2048)
	benchmarkProofGen("128MB_k4096", 4096, 4096, 32768)

	// Verification benchmarks
	benchmarkVerification("1MB_k1024", 1024, 1024, 1024)
	benchmarkVerification("8MB_k4096", 4096, 4096, 2048)
	benchmarkVerification("128MB_k4096", 4096, 4096, 32768)
}
