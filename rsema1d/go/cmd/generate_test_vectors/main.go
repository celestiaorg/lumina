package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"

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

func generateTestVector(name string, k, n, rowSize int, seed int64, proofIndices []int) TestVector {
	fmt.Printf("Generating test vector: %s (k=%d, n=%d, rowSize=%d, seed=%d)\n", name, k, n, rowSize, seed)

	originalData := generateDeterministicData(k, rowSize, seed)

	originalDataHex := make([]string, k)
	for i, row := range originalData {
		originalDataHex[i] = hex.EncodeToString(row)
	}

	config := &rsema1d.Config{
		K:           k,
		N:           n,
		RowSize:     rowSize,
		WorkerCount: 1, // Sequential for determinism
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

func main() {
	testVectors := []TestVector{
		// Small test cases
		generateTestVector("small_4x4", 4, 4, 64, 12345, []int{0, 3, 4, 7}),

		// Medium test cases
		generateTestVector("medium_1MB", 1024, 1024, 1024, 54321, []int{0, 100, 1023, 1024, 1500, 2047}),

		// Large test cases
		generateTestVector("large_8MB", 4096, 4096, 2048, 98765, []int{0, 1000, 4095, 4096, 6000, 8191}),
		generateTestVector("large_128MB_k4096", 4096, 4096, 32768, 87654, []int{0, 2000, 4095, 4096, 6000, 8191}),
		generateTestVector("large_128MB_k8192", 8192, 8192, 16384, 76543, []int{0, 4000, 8191, 8192, 12000, 16383}),

		// Edge cases
		generateTestVector("edge_1x1", 1, 1, 64, 11111, []int{0, 1}),
		generateTestVector("edge_2x2", 2, 2, 64, 22222, []int{0, 1, 2, 3}),

		// Different ratios (powers of 2)
		generateTestVector("ratio_1to3", 4, 12, 64, 33333, []int{0, 3, 4, 8, 15}),
		generateTestVector("ratio_3to1", 12, 4, 64, 44444, []int{0, 5, 11, 12, 15}),

		// Non-power-of-2 test cases
		generateTestVector("non_pow2_k5_n7", 5, 7, 64, 55555, []int{0, 2, 4, 5, 8, 11}),
		generateTestVector("non_pow2_k3_n5", 3, 5, 128, 66666, []int{0, 1, 2, 3, 5, 7}),
		generateTestVector("non_pow2_k7_n9", 7, 9, 256, 77777, []int{0, 3, 6, 7, 10, 15}),
		generateTestVector("non_pow2_k10_n6", 10, 6, 192, 88888, []int{0, 5, 9, 10, 12, 15}),
		generateTestVector("non_pow2_k100_n50", 100, 50, 128, 99999, []int{0, 50, 99, 100, 120, 149}),
		generateTestVector("non_pow2_k1000_n500", 1000, 500, 256, 111222, []int{0, 500, 999, 1000, 1200, 1499}),
	}

	output := TestVectorFile{
		Version:   "2.0-public-api",
		TestCases: testVectors,
	}

	// Write to file
	file, err := os.Create("test_vectors.json")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(output); err != nil {
		panic(err)
	}

	fmt.Printf("\n✅ Generated %d test vectors -> test_vectors.json\n", len(testVectors))
}
