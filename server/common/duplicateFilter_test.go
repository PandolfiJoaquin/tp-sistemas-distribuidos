package common

import "testing"

func TestDuplicateFilterHappy(t *testing.T) {
	filter := NewDuplicateFilter()
	tests := []struct {
		id          int
		expected    bool
		expectedWin []int // Optional: Expected window state
	}{
		{0, true, []int{0}},
		{1, true, []int{1}},
		{2, true, []int{2}},
		{3, true, []int{3}},
		{4, true, []int{4}},
		{1, false, []int{4}}, // Duplicate
		{5, true, []int{5}},
		{4, false, []int{5}}, // Duplicate
	}
	for i, test := range tests {
		got := filter.ShouldAccept(test.id)
		if got != test.expected {
			t.Errorf("[%d] ShouldAccept(%d) = %v; want %v", i, test.id, got, test.expected)
		}

		if test.expectedWin != nil {
			window := filter.GetCurrentWindowState()
			if !equalSlices(window, test.expectedWin) {
				t.Errorf("[%d] GetCurrentWindowState() = %v; want %v", i, window, test.expectedWin)
			}
		}
	}
}

func TestDuplicateFilterIncremental(t *testing.T) {
	filter := NewDuplicateFilter()

	tests := []struct {
		id          int
		expected    bool
		expectedWin []int // Optional: Expected window state
	}{
		{0, true, []int{0}},
		{1, true, []int{1}},
		{3, true, []int{1, 3}},
		{2, true, []int{3}}, // Compact: [1,2,3] → [3]
		{5, true, []int{3, 5}},
		{4, true, []int{5}}, // Compact: [3,4,5] → [5]
		{3, false, nil},     // Duplicate
		{6, true, []int{6}}, // Compact: [5,6] → [6]
		{8, true, []int{6, 8}},
		{7, true, []int{8}}, // Compact: [6,7,8] → [8]
		{9, true, []int{9}}, // Compact: [8,9] → [9]
		{11, true, []int{9, 11}},
		{10, true, []int{11}}, // Compact: [9,10,11] → [11]
		{12, true, []int{12}}, // Compact: [11,12] → [12]
		{13, true, []int{13}}, // Compact: [12,13] → [13]
		{13, false, nil},      // Duplicate
		{14, true, []int{14}}, // Compact: [13,14] → [14]
		{10, false, nil},      // Duplicate (confirmed)
		{15, true, []int{15}}, // Compact: [14,15] → [15]
		{1, false, nil},       // Too old
	}

	for i, test := range tests {
		got := filter.ShouldAccept(test.id)
		if got != test.expected {
			t.Errorf("[%d] ShouldAccept(%d) = %v; want %v", i, test.id, got, test.expected)
		}

		if test.expectedWin != nil {
			window := filter.GetCurrentWindowState()
			if !equalSlices(window, test.expectedWin) {
				t.Errorf("[%d] GetCurrentWindowState() = %v; want %v", i, window, test.expectedWin)
			}
		}
	}
}

func TestDuplicateFilterMoreRange(t *testing.T) {
	filter := NewDuplicateFilter()

	scenarios := []struct {
		id          int
		expected    bool
		expectedWin []int
	}{
		{0, true, []int{0}},
		{10, true, []int{0, 10}},
		{1, true, []int{1, 10}},
		{2, true, []int{2, 10}},   // Compact: [0,1,2,10] → [10]
		{3, true, []int{3, 10}},   // Compact : [3,10] → [3,10]
		{4, true, []int{4, 10}},   // Compact: [3,4,10] → [4,10]
		{10, false, []int{4, 10}}, // Duplicate
		{5, true, []int{5, 10}},   // Compact: [4,5,10] → [5,10]
		{11, true, []int{5, 10, 11}},
		{6, true, []int{6, 10, 11}},  // Compact: [5,6,10,11] → [6,10,11]
		{4, false, []int{6, 10, 11}}, // Duplicate
		{7, true, []int{7, 10, 11}},  // Compact: [6,7,10,11] → [7,10,11]
		{8, true, []int{8, 10, 11}},  // Compact: [7,8,10,11] → [8,10,11]
		{9, true, []int{11}},         // Compact: [8,9,10,11] → [11]
		{12, true, []int{12}},        // Compact: [11,12] → [12]
		{20, true, []int{12, 20}},    // Compact: [12,20] → [20]
		{15, true, []int{12, 15, 20}},
		{14, true, []int{12, 14, 15, 20}},
		{13, true, []int{15, 20}},     // Compact: [12,13,14,15,20] → [15,20]
		{13, false, []int{15, 20}},    // Duplicate
		{21, true, []int{15, 20, 21}}, // Compact: [15,20,21] → [20,21]
	}

	for i, s := range scenarios {
		got := filter.ShouldAccept(s.id)
		if got != s.expected {
			t.Errorf("[%d] ShouldAccept(%d) = %v; want %v",
				i, s.id, got, s.expected)
		}
		if s.expectedWin != nil {
			win := filter.GetCurrentWindowState()
			if !equalSlices(win, s.expectedWin) {
				t.Errorf("[%d] window = %v; want %v",
					i, win, s.expectedWin)
			}
		}
	}
}

func TestDuplicateFilterNegative(t *testing.T) {
	filter := NewDuplicateFilter()
	tests := []struct {
		id       int
		expected bool
	}{
		{-1, false}, // Negative ID should be rejected
		{-10, false},
		{-100, false},
		{0, true},    // Zero is valid
		{1, true},    // Positive IDs should be accepted
		{-10, false}, // Negative again
		{0, false},   // Zero again, should be rejected after first acceptance
	}

	for i, test := range tests {
		got := filter.ShouldAccept(test.id)
		if got != test.expected {
			t.Errorf("[%d] ShouldAccept(%d) = %v; want %v", i, test.id, got, test.expected)
		}
	}
}

// Helper function to compare slices
func equalSlices(a, b []int) bool {
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

func TestDuplicateFilterWithShard(t *testing.T) {
	filter := NewDuplicateFilterWithShards()

	type testCase struct {
		id          int
		shardID     int
		expected    bool
		expectedWin []int
	}
	// We add 0 1 2 2 5 3 to shard 1
	// We add 2 0 3 1 2 to shard 2
	tests := []testCase{
		{id: 0, shardID: 1, expected: true, expectedWin: []int{0}},
		{id: 1, shardID: 1, expected: true, expectedWin: []int{1}},
		{id: 2, shardID: 1, expected: true, expectedWin: []int{2}},
		{id: 2, shardID: 2, expected: true, expectedWin: []int{-1, 2}},
		{id: 2, shardID: 1, expected: false, expectedWin: []int{2}},
		{id: 0, shardID: 2, expected: true, expectedWin: []int{0, 2}},
		{id: 3, shardID: 2, expected: true, expectedWin: []int{0, 2, 3}},
		{id: 5, shardID: 1, expected: true, expectedWin: []int{2, 5}},
		{id: 3, shardID: 1, expected: true, expectedWin: []int{3, 5}},
		{id: 1, shardID: 2, expected: true, expectedWin: []int{3}},
		{id: 2, shardID: 2, expected: false, expectedWin: []int{3}}, // duplicate in shard 2
	}

	for i, test := range tests {
		got := filter.ShouldAccept(test.id, test.shardID)
		if got != test.expected {
			t.Errorf("[%d] ShouldAccept(id=%d, shard=%d) = %v; want %v",
				i, test.id, test.shardID, got, test.expected)
		}

		if test.expectedWin != nil {
			state := filter.GetCurrentWindowsState()
			gotWin := state[test.shardID]
			if !equalSlices(gotWin, test.expectedWin) {
				t.Errorf("[%d] window for shard %d = %v; want %v",
					i, test.shardID, gotWin, test.expectedWin)
			}
		}
	}
}
