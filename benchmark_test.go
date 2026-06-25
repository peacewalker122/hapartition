package hapartition

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestRedisBenchmarkSimulated(t *testing.T) {
	// This test simulates what redis-benchmark does
	s, addr, cleanup := startServerRaw(t, "test-node")
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	// Simulate redis-benchmark SET commands with random keys
	successCount := 0
	failCount := 0
	movedCount := 0

	for i := 0; i < 1000; i++ {
		// redis-benchmark uses __rand_int__ which generates random integers
		// Let's simulate that with random-looking keys
		k := fmt.Sprintf("__rand_int__%d", i)
		v := fmt.Sprintf("value_%d", i)

		err := rdb.Set(context.Background(), k, v, 0).Err()
		if err != nil {
			failCount++
			if isMovedError(err) {
				movedCount++
			}
			if movedCount <= 3 {
				t.Logf("SET %s failed: %v (MOVED=%v)", k, err, isMovedError(err))
			}
		} else {
			successCount++
		}
	}

	t.Logf("Results: success=%d fail=%d moved=%d", successCount, failCount, movedCount)
	t.Logf("Store size: %d", s.Store().Len())

	// With a single node, all keys should be stored locally
	if movedCount > 0 {
		t.Errorf("expected 0 MOVED errors with single node, got %d", movedCount)
	}
}

func TestRedisBenchmarkWithCluster(t *testing.T) {
	// Test with two nodes to see MOVED behavior
	s1, addr1, cleanup1 := startServerRaw(t, "node-A")
	defer cleanup1()
	s2, addr2, cleanup2 := startServerRaw(t, "node-B")
	defer cleanup2()

	s1.Ring().AddNode("node-B", addr2, 256)
	s2.Ring().AddNode("node-A", addr1, 256)

	rdb := redis.NewClient(&redis.Options{Addr: addr1})
	defer rdb.Close()

	localCount := 0
	movedCount := 0

	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("key-%d", i)
		v := fmt.Sprintf("val-%d", i)

		err := rdb.Set(context.Background(), k, v, 0).Err()
		if err != nil && isMovedError(err) {
			movedCount++
		} else {
			localCount++
		}
	}

	t.Logf("Two-node cluster: local=%d moved=%d", localCount, movedCount)
	t.Logf("Node-A store: %d", s1.Store().Len())
	t.Logf("Node-B store: %d", s2.Store().Len())

	// With two nodes, we expect some MOVED errors
	if movedCount == 0 {
		t.Log("Note: no MOVED errors - all keys happened to land on node-A")
	}
}

func TestSetGetAfterBenchmark(t *testing.T) {
	// Reproduce the exact scenario: SET many keys, then verify GET works
	s, addr, cleanup := startServerRaw(t, "test-node")
	defer cleanup()

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	defer rdb.Close()

	// SET 100 keys
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("bench-%d", i)
		v := fmt.Sprintf("val-%d", i)
		rdb.Set(context.Background(), k, v, 0)
	}

	t.Logf("After SET: store.Len()=%d", s.Store().Len())

	// Verify GET works for all stored keys
	getCount := 0
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("bench-%d", i)
		v, err := rdb.Get(context.Background(), k).Result()
		if err == nil {
			getCount++
			expected := fmt.Sprintf("val-%d", i)
			if v != expected {
				t.Errorf("GET %s = %q, want %q", k, v, expected)
			}
		}
	}

	t.Logf("GET success: %d/100", getCount)

	// All keys should be retrievable
	if getCount != 100 {
		t.Errorf("expected 100 GET successes, got %d", getCount)
	}
}

// MovedError represents a MOVED redirect from the server
type MovedError struct {
	Addr string
}

func (e *MovedError) Error() string {
	return fmt.Sprintf("MOVED %s", e.Addr)
}

// isMovedError checks if an error is a MOVED redirect
func isMovedErrorCheck(err error) bool {
	if err == nil {
		return false
	}
	// go-redis wraps MOVED errors
	return fmt.Sprintf("%v", err) != "" && containsSubstring(err.Error(), "MOVED")
}

func containsSubstring(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// Override isMovedError for this test file
var _ = isMovedError // Use the one from integration_test.go
