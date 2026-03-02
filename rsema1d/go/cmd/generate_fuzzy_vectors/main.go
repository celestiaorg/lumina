package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/celestiaorg/rsema1d"
)

type TestVector struct {
	Name   string     `json:"name"`
	Params TestParams `json:"params"`
	Seed   int64      `json:"seed"`

	OriginalData []string `json:"original_data"` // Hex-encoded original rows

	// Public outputs
	Commitment string `json:"commitment"` // The final commitment

	// Proof tests
	ProofTests []ProofTest `json:"proof_tests"`
}

type TestParams struct {
	K       int `json:"k"`
	N       int `json:"n"`
	RowSize int `json:"row_size"`
}

type ProofTest struct {
	Index           int      `json:"index"`
	IsOriginal      bool     `json:"is_original"`
	RowHash         string   `json:"row_hash"`         // SHA256 of the row data
	RowProofHash    string   `json:"row_proof_hash"`   // SHA256 of row Merkle proof
	RLCProofHash    string   `json:"rlc_proof_hash"`   // SHA256 of RLC proof (if original)
	RowData         string   `json:"row_data"`         // Raw row bytes (hex)
	RowProof        []string `json:"row_proof"`        // Row Merkle proof nodes (hex)
	RLCProof        []string `json:"rlc_proof"`        // RLC Merkle proof nodes (hex, original rows only)
	StandaloneProof bool     `json:"standalone_proof"` // Whether this is a standalone proof
}

type TestVectorFile struct {
	Version   string       `json:"version"`
	TestCases []TestVector `json:"test_cases"`
}

func generateDeterministicData(k, rowSize int, seed int64) [][]byte {
	rng := rand.New(rand.NewSource(seed))
	data := make([][]byte, k)
	for i := 0; i < k; i++ {
		data[i] = make([]byte, rowSize)
		for j := 0; j < rowSize; j++ {
			data[i][j] = byte(rng.Intn(256))
		}
	}
	return data
}

func hashBytes(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func hashMerkleProof(proof [][]byte) string {
	h := sha256.New()
	for _, node := range proof {
		h.Write(node)
	}
	return hex.EncodeToString(h.Sum(nil))
}

func encodeProofNodes(proof [][]byte) []string {
	encoded := make([]string, len(proof))
	for i, node := range proof {
		encoded[i] = hex.EncodeToString(node)
	}
	return encoded
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

func nextPowerOfTwo(n int) int {
	if n <= 1 {
		return 1
	}
	p := 1
	for p < n {
		p <<= 1
	}
	return p
}

func randomN(rng *rand.Rand) int {
	// Heavily sample small/medium values for fast default tests, but keep some large values.
	p := rng.Float64()
	switch {
	case p < 0.70:
		return rng.Intn(256) + 1
	case p < 0.95:
		return rng.Intn(2048-256) + 257
	default:
		return rng.Intn(32768-2048) + 2049
	}
}

func randomValidKN(rng *rand.Rand, maxTotalRows int) (int, int) {
	if maxTotalRows < 2 {
		maxTotalRows = 2
	}

	for {
		n := randomN(rng)
		np2 := nextPowerOfTwo(n)

		// Constraints:
		// 1) k + n <= 65536 (Go config validation)
		// 2) k + next_pow_2(n) < 65536 (cross-validation target)
		// 3) k + n <= maxTotalRows (runtime cap for default test speed)
		kMaxByKN := 65536 - n
		kMaxByNp2 := 65535 - np2
		kMaxByTotal := maxTotalRows - n
		kMax := min(kMaxByKN, min(kMaxByNp2, kMaxByTotal))
		if kMax < 1 {
			continue
		}

		k := rng.Intn(kMax) + 1

		// Bias towards unequal pairs.
		if k == n && rng.Float64() < 0.90 {
			continue
		}

		return k, n
	}
}

func generateTestVector(name string, k, n, rowSize int, seed int64, proofIndices []int) TestVector {
	fmt.Printf("Generating: %s (k=%d, n=%d, rowSize=%d, seed=%d)\n", name, k, n, rowSize, seed)

	originalData := generateDeterministicData(k, rowSize, seed)

	originalDataHex := make([]string, k)
	for i, row := range originalData {
		originalDataHex[i] = hex.EncodeToString(row)
	}

	config := &rsema1d.Config{
		K:           k,
		N:           n,
		RowSize:     rowSize,
		WorkerCount: 1,
	}

	// Use only public API
	extData, commitment, _, err := rsema1d.Encode(originalData, config)
	if err != nil {
		panic(fmt.Sprintf("Failed to encode: %v", err))
	}

	var proofTests []ProofTest
	for _, idx := range proofIndices {
		// Try standalone proof first (only works for original rows)
		standaloneProof, err := extData.GenerateStandaloneProof(idx)
		if err == nil {
			// Original row with standalone proof
			proofTests = append(proofTests, ProofTest{
				Index:           idx,
				IsOriginal:      true,
				StandaloneProof: true,
				RowHash:         hashBytes(standaloneProof.RowProof.Row),
				RowProofHash:    hashMerkleProof(standaloneProof.RowProof.RowProof),
				RLCProofHash:    hashMerkleProof(standaloneProof.RLCProof),
				RowData:         hex.EncodeToString(standaloneProof.RowProof.Row),
				RowProof:        encodeProofNodes(standaloneProof.RowProof.RowProof),
				RLCProof:        encodeProofNodes(standaloneProof.RLCProof),
			})
		} else {
			// Extended row - use row proof only
			rowProof, err := extData.GenerateRowProof(idx)
			if err != nil {
				panic(fmt.Sprintf("Failed to generate proof for index %d: %v", idx, err))
			}

			proofTests = append(proofTests, ProofTest{
				Index:           idx,
				IsOriginal:      false,
				StandaloneProof: false,
				RowHash:         hashBytes(rowProof.Row),
				RowProofHash:    hashMerkleProof(rowProof.RowProof),
				RowData:         hex.EncodeToString(rowProof.Row),
				RowProof:        encodeProofNodes(rowProof.RowProof),
				RLCProof:        []string{},
			})
		}
	}

	return TestVector{
		Name:         name,
		Params:       TestParams{K: k, N: n, RowSize: rowSize},
		Seed:         seed,
		OriginalData: originalDataHex,
		Commitment:   hex.EncodeToString(commitment[:]),
		ProofTests:   proofTests,
	}
}

func randomProofIndices(rng *rand.Rand, k, n, count int) []int {
	totalRows := k + n
	if count > totalRows {
		count = totalRows
	}

	indices := make([]int, 0, count)
	seen := make(map[int]struct{}, count)

	add := func(idx int) {
		if idx < 0 || idx >= totalRows {
			return
		}
		if _, ok := seen[idx]; ok {
			return
		}
		seen[idx] = struct{}{}
		indices = append(indices, idx)
	}

	// Ensure we test both original and parity domains when possible.
	if k > 0 {
		add(rng.Intn(k))
	}
	if n > 0 {
		add(k + rng.Intn(n))
	}

	for len(indices) < count {
		add(rng.Intn(totalRows))
	}

	return indices
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func main() {
	numTests := 250
	if len(os.Args) > 1 {
		fmt.Sscanf(os.Args[1], "%d", &numTests)
	}
	maxTotalRows := 4096
	if len(os.Args) > 2 {
		fmt.Sscanf(os.Args[2], "%d", &maxTotalRows)
	}

	type fixedCase struct {
		name    string
		k       int
		n       int
		rowSize int
		seed    int64
	}
	fixedCases := []fixedCase{
		{
			name:    "fuzzy_fixed_k4096_n12288_r64",
			k:       4096,
			n:       4096 * 3,
			rowSize: 64,
			seed:    4096012288064,
		},
		{
			name:    "fuzzy_fixed_k4096_n12288_r128",
			k:       4096,
			n:       4096 * 3,
			rowSize: 128,
			seed:    4096012288128,
		},
		{
			name:    "fuzzy_fixed_k4096_n12288_r256",
			k:       4096,
			n:       4096 * 3,
			rowSize: 256,
			seed:    4096012288256,
		},
		{
			name:    "fuzzy_fixed_k4096_n12288_r512",
			k:       4096,
			n:       4096 * 3,
			rowSize: 512,
			seed:    4096012288512,
		},
	}

	fmt.Printf("Generating %d fuzzy test vectors (max_total_rows=%d) + %d fixed high-scale cases...\n", numTests, maxTotalRows, len(fixedCases))

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	var testVectors []TestVector

	// Always include high-scale Fibre-relevant cases.
	for _, c := range fixedCases {
		proofRng := rand.New(rand.NewSource(c.seed ^ 0x5DEECE66D))
		testVectors = append(testVectors, generateTestVector(
			c.name,
			c.k,
			c.n,
			c.rowSize,
			c.seed,
			randomProofIndices(proofRng, c.k, c.n, 6),
		))
	}

	// Valid row sizes (must be multiple of 64)
	rowSizes := []int{64, 128, 256, 512, 1024, 2048}

	for i := 0; i < numTests; i++ {
		k, n := randomValidKN(rng, maxTotalRows)

		rowSize := rowSizes[rng.Intn(len(rowSizes))]
		seed := rng.Int63()

		// Generate random proof indices
		totalRows := k + n
		numProofs := min(6, totalRows)
		proofIndices := randomProofIndices(rng, k, n, numProofs)

		name := fmt.Sprintf("fuzzy_random_%d_k%d_n%d_r%d", i, k, n, rowSize)
		testVectors = append(testVectors, generateTestVector(name, k, n, rowSize, seed, proofIndices))
	}

	output := TestVectorFile{
		Version:   "2.0-public-api",
		TestCases: testVectors,
	}

	file, err := os.Create("fuzzy_test_vectors.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		panic(err)
	}

	fmt.Printf("\n✅ Generated %d fuzzy test vectors -> fuzzy_test_vectors.json\n", len(testVectors))
}
