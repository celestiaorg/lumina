package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"runtime"
	"time"

	rsema1d "github.com/celestiaorg/celestia-app/v10/pkg/rsema1d"
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

// makeEncodeRows returns the K+N row slice Coder.Encode expects: original
// data in rows[:K], allocated parity rows in rows[K:].
func makeEncodeRows(data [][]byte, k, n, rowSize int) [][]byte {
	rows := make([][]byte, k+n)
	copy(rows, data)
	for i := k; i < k+n; i++ {
		rows[i] = make([]byte, rowSize)
	}
	return rows
}

// zeroParity re-zeroes rows[K:], required by Coder.Encode before each call.
func zeroParity(rows [][]byte, k int) {
	for _, row := range rows[k:] {
		clear(row)
	}
}

func benchmarkEncode(name string, k, n, rowSize int) {
	workers := runtime.GOMAXPROCS(0)
	data := makeTestData(k, rowSize)
	config := &rsema1d.Config{
		K:           k,
		N:           n,
		WorkerCount: workers,
	}
	coder, err := rsema1d.NewCoder(config)
	if err != nil {
		panic(err)
	}
	rows := makeEncodeRows(data, k, n, rowSize)

	// Warmup
	for i := 0; i < 10; i++ {
		zeroParity(rows, k)
		coder.Encode(rows)
	}

	// Measure
	iterations := 100
	start := time.Now()
	for i := 0; i < iterations; i++ {
		zeroParity(rows, k)
		_, err := coder.Encode(rows)
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
	workers := runtime.GOMAXPROCS(0)
	data := makeTestData(k, rowSize)
	config := &rsema1d.Config{
		K:           k,
		N:           n,
		WorkerCount: workers,
	}

	coder, err := rsema1d.NewCoder(config)
	if err != nil {
		panic(err)
	}
	extData, err := coder.Encode(makeEncodeRows(data, k, n, rowSize))
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

func benchmarkVerification(name, mode string, k, n, rowSize int) {
	workers := runtime.GOMAXPROCS(0)
	data := makeTestData(k, rowSize)
	config := &rsema1d.Config{
		K:           k,
		N:           n,
		WorkerCount: workers,
	}

	coder, err := rsema1d.NewCoder(config)
	if err != nil {
		panic(err)
	}
	extData, err := coder.Encode(makeEncodeRows(data, k, n, rowSize))
	if err != nil {
		panic(err)
	}
	commitment := extData.Commitment()

	var verify func() error
	switch mode {
	case "cached":
		proof, err := extData.GenerateRowProof(0)
		if err != nil {
			panic(err)
		}
		proofs := []*rsema1d.RowProof{proof}
		verifier, err := rsema1d.NewVerifier(config)
		if err != nil {
			panic(err)
		}
		if err := verifier.Verify(commitment, proofs, extData.RLC()); err != nil {
			panic(err)
		}
		verify = func() error { return verifier.VerifyShared(commitment, proofs) }
	case "standalone":
		proof, err := extData.GenerateStandaloneProof(0)
		if err != nil {
			panic(err)
		}
		verify = func() error { return rsema1d.VerifyStandaloneProof(proof, commitment, config) }
	default:
		panic("unknown verification mode: " + mode)
	}

	// Warmup
	for i := 0; i < 100; i++ {
		if err := verify(); err != nil {
			panic(err)
		}
	}

	// Measure
	iterations := 1000
	start := time.Now()
	for i := 0; i < iterations; i++ {
		err := verify()
		if err != nil {
			panic(err)
		}
	}
	elapsed := time.Since(start)
	avgTime := elapsed / time.Duration(iterations)

	fmt.Printf("%s: %.4fms\n", name, float64(avgTime.Nanoseconds())/1000000.0)
}

func main() {
	pattern := flag.String("bench", ".", "regular expression selecting benchmark names")
	flag.Parse()
	filter, err := regexp.Compile(*pattern)
	if err != nil {
		fmt.Fprintf(os.Stderr, "invalid -bench pattern: %v\n", err)
		os.Exit(2)
	}
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}

	fmt.Println("# Go Benchmark Results")
	fmt.Printf("# WorkerCount = GOMAXPROCS = %d\n", runtime.GOMAXPROCS(0))

	run := func(name string, k, n, rowSize int, benchmark func()) {
		if !filter.MatchString(name) {
			return
		}
		fmt.Printf("%s: K=%d N=%d row_size=%d workers=%d\n", name, k, n, rowSize, runtime.GOMAXPROCS(0))
		benchmark()
	}

	// 128 MiB encode benchmarks matching Rust configurations.
	for _, tc := range []struct {
		name          string
		k, n, rowSize int
	}{
		{"128MB_k1024_n1024", 1024, 1024, 131072},
		{"128MB_k4096_n12288", 4096, 12288, 32768},
		{"128MB_k8192_n24576", 8192, 24576, 16384},
	} {
		run("Encode_"+tc.name, tc.k, tc.n, tc.rowSize, func() {
			benchmarkEncode(tc.name, tc.k, tc.n, tc.rowSize)
		})
	}

	// Proof generation benchmarks
	run("ProofGen_128MB_k4096_n12288", 4096, 12288, 32768, func() {
		benchmarkProofGen("128MB_k4096_n12288", 4096, 12288, 32768)
	})

	// Verification benchmarks
	for _, mode := range []string{"cached", "standalone"} {
		name := "verification/" + mode + "/128MB_k4096_n12288"
		run(name, 4096, 12288, 32768, func() {
			benchmarkVerification(name, mode, 4096, 12288, 32768)
		})
	}
}
