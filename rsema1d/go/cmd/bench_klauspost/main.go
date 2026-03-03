package main

import (
	"fmt"
	"os"
	"runtime"
	"time"

	kp "github.com/klauspost/reedsolomon"
)

type benchCase struct {
	name    string
	k       int
	n       int
	rowSize int
	iters   int
}

func makeCases(quick bool) []benchCase {
	if quick {
		return []benchCase{
			{name: "128KB_k1024_n3072", k: 1024, n: 3072, rowSize: 128, iters: 5},
			{name: "1MB_k1024_n3072", k: 1024, n: 3072, rowSize: 1024, iters: 3},
		}
	}
	return []benchCase{
		{name: "128KB_k1024_n3072", k: 1024, n: 3072, rowSize: 128, iters: 200},
		{name: "1MB_k1024_n3072", k: 1024, n: 3072, rowSize: 1024, iters: 100},
		{name: "1MB_k4096_n12288", k: 4096, n: 12288, rowSize: 256, iters: 40},
		{name: "8MB_k4096_n12288", k: 4096, n: 12288, rowSize: 2048, iters: 10},
		{name: "128MB_k4096_n12288", k: 4096, n: 12288, rowSize: 32768, iters: 2},
	}
}

func makeOriginal(k, rowSize int) [][]byte {
	rows := make([][]byte, k)
	for i := 0; i < k; i++ {
		row := make([]byte, rowSize)
		for j := 0; j < rowSize; j++ {
			row[j] = byte((i + j) & 0xff)
		}
		rows[i] = row
	}
	return rows
}

func makeShards(k, n, rowSize int, original [][]byte) [][]byte {
	shards := make([][]byte, k+n)
	copy(shards[:k], original)
	for i := 0; i < n; i++ {
		shards[k+i] = make([]byte, rowSize)
	}
	return shards
}

func runCase(c benchCase) {
	original := makeOriginal(c.k, c.rowSize)
	shards := makeShards(c.k, c.n, c.rowSize, original)
	sink := byte(0)

	for i := 0; i < 2; i++ {
		enc, err := kp.New(c.k, c.n)
		if err != nil {
			panic(err)
		}
		for p := c.k; p < c.k+c.n; p++ {
			clear(shards[p])
		}
		if err := enc.Encode(shards); err != nil {
			panic(err)
		}
		if c.n > 0 {
			sink ^= shards[c.k][0]
		}
	}

	start := time.Now()
	for i := 0; i < c.iters; i++ {
		enc, err := kp.New(c.k, c.n)
		if err != nil {
			panic(err)
		}
		for p := c.k; p < c.k+c.n; p++ {
			clear(shards[p])
		}
		if err := enc.Encode(shards); err != nil {
			panic(err)
		}
		if c.n > 0 {
			sink ^= shards[c.k][0]
		}
	}
	elapsed := time.Since(start)
	_ = sink

	avg := elapsed.Seconds() / float64(c.iters)
	mibPerSec := float64(c.k*c.rowSize) / avg / 1024.0 / 1024.0
	fmt.Printf(
		"KlauspostRS_%-22s avg=%8.4fms throughput=%8.2f MiB/s iters=%d\n",
		c.name,
		avg*1000.0,
		mibPerSec,
		c.iters,
	)
}

func main() {
	quick := os.Getenv("RS_BENCH_QUICK") == "1"
	cases := makeCases(quick)

	fmt.Println("# Go klauspost/reedsolomon")
	fmt.Printf("# mode=%s GOMAXPROCS=%d\n", map[bool]string{true: "quick", false: "full"}[quick], runtime.GOMAXPROCS(0))
	for _, c := range cases {
		runCase(c)
	}
}
