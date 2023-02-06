// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"math"

	enc "github.com/jbrjake/sketches-go/ddsketch/encoding"
)

// CollapsingLowestDenseStore is a dynamically growing contiguous (non-sparse) store.
// The lower bins get combined so that the total number of bins do not exceed maxNumBins.
type CollapsingLowestDenseStore struct {
	DenseStore
	MaxNumBins  int
	IsCollapsed bool
}

func NewCollapsingLowestDenseStore(maxNumBins int) *CollapsingLowestDenseStore {
	// Bins are not allocated until values are added.
	// When the first value is added, a small number of bins are allocated. The number of bins will
	// grow as needed up to maxNumBins.
	return &CollapsingLowestDenseStore{
		DenseStore:  DenseStore{_MinIndex: math.MaxInt32, _MaxIndex: math.MinInt32},
		MaxNumBins:  maxNumBins,
		IsCollapsed: false,
	}
}

func (s *CollapsingLowestDenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *CollapsingLowestDenseStore) AddBin(bin Bin) {
	index := bin.Index()
	count := bin.Count()
	if count == 0 {
		return
	}
	s.AddWithCount(index, count)
}

func (s *CollapsingLowestDenseStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	}
	arrayIndex := s.normalize(index)
	s._Bins[arrayIndex] += count
	s.Count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *CollapsingLowestDenseStore) normalize(index int) int {
	if index < s._MinIndex {
		if s.IsCollapsed {
			return 0
		} else {
			s.extendRange(index, index)
			if s.IsCollapsed {
				return 0
			}
		}
	} else if index > s._MaxIndex {
		s.extendRange(index, index)
	}
	return index - s.Offset
}

func (s *CollapsingLowestDenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	return min(s.DenseStore.getNewLength(newMinIndex, newMaxIndex), s.MaxNumBins)
}

func (s *CollapsingLowestDenseStore) extendRange(newMinIndex, newMaxIndex int) {
	newMinIndex = min(newMinIndex, s._MinIndex)
	newMaxIndex = max(newMaxIndex, s._MaxIndex)
	if s.IsEmpty() {
		initialLength := s.getNewLength(newMinIndex, newMaxIndex)
		s._Bins = append(s._Bins, make([]float64, initialLength)...)
		s.Offset = newMinIndex
		s._MinIndex = newMinIndex
		s._MaxIndex = newMaxIndex
		s.adjust(newMinIndex, newMaxIndex)
	} else if newMinIndex >= s.Offset && newMaxIndex < s.Offset+len(s._Bins) {
		s._MinIndex = newMinIndex
		s._MaxIndex = newMaxIndex
	} else {
		// To avoid shifting too often when nearing the capacity of the array,
		// we may grow it before we actually reach the capacity.
		newLength := s.getNewLength(newMinIndex, newMaxIndex)
		if newLength > len(s._Bins) {
			s._Bins = append(s._Bins, make([]float64, newLength-len(s._Bins))...)
		}
		s.adjust(newMinIndex, newMaxIndex)
	}
}

// Adjust bins, offset, minIndex and maxIndex, without resizing the bins slice in order to make it fit the
// specified range.
func (s *CollapsingLowestDenseStore) adjust(newMinIndex, newMaxIndex int) {
	if newMaxIndex-newMinIndex+1 > len(s._Bins) {
		// The range of indices is too wide, buckets of lowest indices need to be collapsed.
		newMinIndex = newMaxIndex - len(s._Bins) + 1
		if newMinIndex >= s._MaxIndex {
			// There will be only one non-empty bucket.
			s._Bins = make([]float64, len(s._Bins))
			s.Offset = newMinIndex
			s._MinIndex = newMinIndex
			s._Bins[0] = s.Count
		} else {
			shift := s.Offset - newMinIndex
			if shift < 0 {
				// Collapse the buckets.
				n := float64(0)
				for i := s._MinIndex; i < newMinIndex; i++ {
					n += s._Bins[i-s.Offset]
				}
				s.resetBins(s._MinIndex, newMinIndex-1)
				s._Bins[newMinIndex-s.Offset] += n
				s._MinIndex = newMinIndex
				// Shift the buckets to make room for newMaxIndex.
				s.shiftCounts(shift)
			} else {
				// Shift the buckets to make room for newMinIndex.
				s.shiftCounts(shift)
				s._MinIndex = newMinIndex
			}
		}
		s._MaxIndex = newMaxIndex
		s.IsCollapsed = true
	} else {
		s.centerCounts(newMinIndex, newMaxIndex)
	}
}

func (s *CollapsingLowestDenseStore) MergeWith(other Store) {
	if other.IsEmpty() {
		return
	}
	o, ok := other.(*CollapsingLowestDenseStore)
	if !ok {
		other.ForEach(func(index int, count float64) (stop bool) {
			s.AddWithCount(index, count)
			return false
		})
		return
	}
	if o._MinIndex < s._MinIndex || o._MaxIndex > s._MaxIndex {
		s.extendRange(o._MinIndex, o._MaxIndex)
	}
	idx := o._MinIndex
	for ; idx < s._MinIndex && idx <= o._MaxIndex; idx++ {
		s._Bins[0] += o._Bins[idx-o.Offset]
	}
	for ; idx < o._MaxIndex; idx++ {
		s._Bins[idx-s.Offset] += o._Bins[idx-o.Offset]
	}
	// This is a separate test so that the comparison in the previous loop is strict (<) and handles
	// store._MaxIndex = Integer.MAX_VALUE.
	if idx == o._MaxIndex {
		s._Bins[idx-s.Offset] += o._Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *CollapsingLowestDenseStore) Copy() Store {
	bins := make([]float64, len(s._Bins))
	copy(bins, s._Bins)
	return &CollapsingLowestDenseStore{
		DenseStore: DenseStore{
			_Bins:     bins,
			Count:     s.Count,
			Offset:    s.Offset,
			_MinIndex: s._MinIndex,
			_MaxIndex: s._MaxIndex,
		},
		MaxNumBins:  s.MaxNumBins,
		IsCollapsed: s.IsCollapsed,
	}
}

func (s *CollapsingLowestDenseStore) Clear() {
	s.DenseStore.Clear()
	s.IsCollapsed = false
}

func (s *CollapsingLowestDenseStore) DecodeAndMergeWith(r *[]byte, encodingMode enc.SubFlag) error {
	return DecodeAndMergeWith(s, r, encodingMode)
}

var _ Store = (*CollapsingLowestDenseStore)(nil)

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
