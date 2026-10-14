package merkle

import (
	"crypto/sha256"
	"encoding/binary"
	"hash/fnv"
	"math"
	"sort"

	"github.com/peacewalker122/hapartition/pkg/store"
)

const defaultNumLeaves = 64

// maxUint64 is 2^64 - 1, the upper bound of the key hash space.
var maxUint64 = uint64(math.MaxUint64)

// Tree is a Merkle tree over the store's key space, partitioning 64-bit key
// hashes into equal-width leaves. Internal nodes form a complete binary tree
// stored in a flat array for efficient comparison.
//
//	numLeaves = 8  →  totalNodes = 15
//	                   ┌─── 0 ───┐
//	                ┌──1──┐     ┌──2──┐
//	               3     4     5     6
//	               7  8  9 10 11 12 13 14   ← leaves
//
// Children of node i: 2*i+1, 2*i+2.
// Leaves start at index firstLeaf = numLeaves - 1.
type Tree struct {
	numLeaves int
	hashes    [][]byte // hashes[0] = root
	keyCounts []int    // keyCounts[i] = keys in subtree i
}

// Build constructs a Merkle tree with numLeaves leaves by:
//  1. Iterating the store and bucketing entries by FNV-64a key hash.
//  2. Computing SHA-256 leaf hashes from sorted bucket contents.
//  3. Building internal nodes bottom-up: hash(left || right).
//
// numLeaves must be a power of two; if not, it is rounded up to the next
// power of two (or left at defaultNumLeaves if zero/negative).
func Build(st *store.Store, numLeaves int) *Tree {
	nl := roundUpPowerOfTwo(numLeaves)
	if nl <= 0 {
		nl = defaultNumLeaves
	}

	// Bucket entries by key hash.
	buckets := make([][]kv, nl)
	st.Iterate(func(key string, e store.Entry) bool {
		h := hashKey(key)
		idx := int(h / (maxUint64/uint64(nl) + 1))
		if idx >= nl {
			idx = nl - 1
		}
		buckets[idx] = append(buckets[idx], kv{key, e.Value, e.Version})
		return true
	})

	// Sort each bucket by key for deterministic leaf hashes.
	for i := range buckets {
		sort.Slice(buckets[i], func(a, b int) bool {
			return buckets[i][a].key < buckets[i][b].key
		})
	}

	// Build complete binary tree.
	totalNodes := 2*nl - 1
	firstLeaf := nl - 1
	hashes := make([][]byte, totalNodes)
	keyCounts := make([]int, totalNodes)

	// Leaf hashes.
	for i := 0; i < nl; i++ {
		hashes[firstLeaf+i] = leafHash(buckets[i])
		keyCounts[firstLeaf+i] = len(buckets[i])
	}

	// Internal nodes, bottom-up.
	for i := firstLeaf - 1; i >= 0; i-- {
		left := hashes[2*i+1]
		right := hashes[2*i+2]
		h := sha256.New()
		h.Write(left)
		h.Write(right)
		hashes[i] = h.Sum(nil)
		keyCounts[i] = keyCounts[2*i+1] + keyCounts[2*i+2]
	}

	return &Tree{
		numLeaves: nl,
		hashes:    hashes,
		keyCounts: keyCounts,
	}
}

// RootHash returns the Merkle root hash.
func (t *Tree) RootHash() []byte { return t.hashes[0] }

// NumLeaves returns the number of leaf nodes.
func (t *Tree) NumLeaves() int { return t.numLeaves }

// LeafHash returns the hash of leaf i (0-indexed).
func (t *Tree) LeafHash(i int) []byte {
	return t.hashes[t.numLeaves-1+i]
}

// LeafKeyCount returns the number of keys in leaf i.
func (t *Tree) LeafKeyCount(i int) int {
	return t.keyCounts[t.numLeaves-1+i]
}

// LeafBounds returns the (inclusive, exclusive) hash range for leaf i.
func LeafBounds(numLeaves, leafIdx int) (uint64, uint64) {
	step := maxUint64/uint64(numLeaves) + 1
	start := uint64(leafIdx) * step
	end := uint64(leafIdx+1) * step
	if leafIdx == numLeaves-1 {
		end = maxUint64
	}
	return start, end
}

// Diff compares two trees leaf-by-leaf and returns indices of leaves whose
// hashes differ. Trees must have the same NumLeaves; if not, returns nil.
func Diff(a, b *Tree) []int {
	if a.numLeaves != b.numLeaves {
		return nil
	}
	var diffs []int
	firstLeaf := a.numLeaves - 1
	for i := 0; i < a.numLeaves; i++ {
		if !bytesEqual(a.hashes[firstLeaf+i], b.hashes[firstLeaf+i]) {
			diffs = append(diffs, i)
		}
	}
	return diffs
}

// LeafEntries returns all store entries whose key hash falls within the
// given leaf's range. O(N) scan — for production, cache these at build time.
func LeafEntries(st *store.Store, numLeaves, leafIdx int) []store.Entry {
	start, end := LeafBounds(numLeaves, leafIdx)
	var entries []store.Entry
	st.Iterate(func(key string, e store.Entry) bool {
		h := hashKey(key)
		if inLeafRange(h, start, end, leafIdx == numLeaves-1) {
			entries = append(entries, e)
		}
		return true
	})
	return entries
}
func inLeafRange(hash, start, end uint64, final bool) bool {
	return hash >= start && (hash < end || final && hash == maxUint64)
}

// --- internal types and helpers ---

type kv struct {
	key, value string
	ver        int64
}

// hashKey returns the FNV-64a hash of the key.
func hashKey(key string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(key))
	return h.Sum64()
}

// leafHash computes SHA-256 over all entries in a bucket.
// Format per entry: len(key) || key || len(value) || value || version (8 bytes BE).
func leafHash(entries []kv) []byte {
	h := sha256.New()
	var buf [8]byte
	for _, e := range entries {
		// Key length + key
		binary.BigEndian.PutUint32(buf[:4], uint32(len(e.key)))
		h.Write(buf[:4])
		h.Write([]byte(e.key))
		// Value length + value
		binary.BigEndian.PutUint32(buf[:4], uint32(len(e.value)))
		h.Write(buf[:4])
		h.Write([]byte(e.value))
		// Version
		binary.BigEndian.PutUint64(buf[:8], uint64(e.ver))
		h.Write(buf[:8])
	}
	return h.Sum(nil)
}

// roundUpPowerOfTwo rounds n up to the next power of two.
// Returns 0 for n <= 0.
func roundUpPowerOfTwo(n int) int {
	if n <= 0 {
		return 0
	}
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	return n + 1
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
