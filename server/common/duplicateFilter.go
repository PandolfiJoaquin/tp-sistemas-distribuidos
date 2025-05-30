package common

import "sort"

// DuplicateFilter is a filter that accepts unique IDs in a sliding window.
// It maintains a sorted window of accepted IDs and rejects duplicates.
// The window is shortened to the position X if all the IDs are consecutive from the start to X.
//
// Precondition: IDs must be non-negative integers starting from 0.
//
// Example we have window [1,3,5] Meaning that 0,1,3,5 were accepted.
// If 2 arrives it will be added and the window will become [3,5]. Compacting Because 1,2,3 are consecutive.
// With this new window we know that ids from 0-3 are already accepted.
type DuplicateFilter struct {
    window []int
}

// NewDuplicateFilter initializes a new DuplicateFilter with an empty window.
func NewDuplicateFilter() *DuplicateFilter {
	return &DuplicateFilter{
		window: []int{-1},
	}
}

// ShouldAccept returns false for duplicates and bad IDs (negative IDs).
// returns true when a new ID is accepted and updates the window accordingly.
func (f *DuplicateFilter) ShouldAccept(id int) bool {
	if len(f.window) > 0 && id < f.window[0] {
		return false
	}

	idx := sort.SearchInts(f.window, id)
	if idx < len(f.window) && f.window[idx] == id {
		return false
	}

	f.window = append(f.window, 0)
	copy(f.window[idx+1:], f.window[idx:])
	f.window[idx] = id

	for i := 1; i <= len(f.window); i++ {
		if i == len(f.window) || f.window[i] != f.window[i-1]+1 {
			f.window = f.window[i-1:]
			break
		}
	}

	return true
}

// GetCurrentWindowState returns a copy of the current window state.
// This is done for saving the state of the filter at any point in time.
func (f *DuplicateFilter) GetCurrentWindowState() []int {
	win := make([]int, len(f.window))
	copy(win, f.window)
	return win
}

// DuplicateFilterWithShards is a filter that manages multiple shards of DuplicateFilter.
// It applies the same logic as DuplicateFilter but allows for separate windows per shard.
type DuplicateFilterWithShards struct {
	shards map[int]*DuplicateFilter
}

func NewDuplicateFilterWithShards() *DuplicateFilterWithShards {
	return &DuplicateFilterWithShards{
		shards: make(map[int]*DuplicateFilter),
	}
}

// ShouldAccept checks if an ID should be accepted in the specified shard.
// Check ShouldAccept in the DuplicateFilter for the logic of accepting IDs.
func (f *DuplicateFilterWithShards) ShouldAccept(id int, shardID int) bool {
	if id < 0 {
		return false // Negative IDs are not accepted
	}

	if _, exists := f.shards[shardID]; !exists {
		f.shards[shardID] = NewDuplicateFilter()
	}

	return f.shards[shardID].ShouldAccept(id)
}

// GetCurrentWindowsState GetCurrentWindowState returns the current state of all shards.
// It returns a map where keys are shard IDs and values are the current window states of those shards.
func (f *DuplicateFilterWithShards) GetCurrentWindowsState() map[int][]int {
	winStates := make(map[int][]int)
	for shardID, filter := range f.shards {
		winStates[shardID] = filter.GetCurrentWindowState()
	}
	return winStates
}
