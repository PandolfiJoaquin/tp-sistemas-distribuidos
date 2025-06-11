package common

import (
	"log/slog"
	"sort"
)

// DuplicateFilter is a filter that accepts unique IDs in a sliding Window.
// It maintains a sorted Window of accepted IDs and rejects duplicates.
// The Window is shortened to the position X if all the IDs are consecutive from the start to X.
//
// Precondition: IDs must be non-negative integers starting from 0.
//
// Example we have Window [1,3,5] Meaning that 0,1,3,5 were accepted.
// If 2 arrives it will be added and the Window will become [3,5]. Compacting Because 1,2,3 are consecutive.
// With this new Window we know that ids from 0-3 are already accepted.
type DuplicateFilter struct {
	Window []int `json:"window"`
}

// NewDuplicateFilter initializes a new DuplicateFilter with an empty Window.
func NewDuplicateFilter() *DuplicateFilter {
	return &DuplicateFilter{
		Window: []int{-1},
	}
}

// Accept returns false for duplicates and bad IDs (negative IDs).
// returns true when a new ID is accepted and updates the Window accordingly.
func (f *DuplicateFilter) Accept(id int) bool {
	if len(f.Window) > 0 && id < f.Window[0] {
		slog.Info("filtering message")
		return false
	}

	idx := sort.SearchInts(f.Window, id)
	if idx < len(f.Window) && f.Window[idx] == id {
		slog.Info("filtering message")
		return false
	}

	f.Window = append(f.Window, 0)
	copy(f.Window[idx+1:], f.Window[idx:])
	f.Window[idx] = id

	for i := 1; i <= len(f.Window); i++ {
		if i == len(f.Window) || f.Window[i] != f.Window[i-1]+1 {
			f.Window = f.Window[i-1:]
			break
		}
	}
	slog.Info("received New Message")
	return true
}

// GetCurrentWindowState returns a copy of the current Window state.
// This is done for saving the state of the filter at any point in time.
func (f *DuplicateFilter) GetCurrentWindowState() []int {
	win := make([]int, len(f.Window))
	copy(win, f.Window)
	return win
}

// DuplicateFilterWithShards is a filter that manages multiple shards of DuplicateFilter.
// It applies the same logic as DuplicateFilter but allows for separate windows per shard.
type DuplicateFilterWithShards struct {
	Shards map[int]*DuplicateFilter `json:"shards"`
}

func NewDuplicateFilterWithShards() *DuplicateFilterWithShards {
	return &DuplicateFilterWithShards{
		Shards: make(map[int]*DuplicateFilter),
	}
}

// Accept checks if an ID should be accepted in the specified shard.
// Check Accept in the DuplicateFilter for the logic of accepting IDs.
func (f *DuplicateFilterWithShards) Accept(id int, shardID int) bool {
	if id < 0 {
		return false // Negative IDs are not accepted
	}

	if _, exists := f.Shards[shardID]; !exists {
		f.Shards[shardID] = NewDuplicateFilter()
	}

	return f.Shards[shardID].Accept(id)
}

// GetCurrentWindowsState GetCurrentWindowState returns the current state of all shards.
// It returns a map where keys are shard IDs and values are the current Window states of those shards.
func (f *DuplicateFilterWithShards) GetCurrentWindowsState() map[int][]int {
	winStates := make(map[int][]int)
	for shardID, filter := range f.Shards {
		winStates[shardID] = filter.GetCurrentWindowState()
	}
	return winStates
}
