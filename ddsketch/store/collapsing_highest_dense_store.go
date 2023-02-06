// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"math"

	enc "github.com/jbrjake/sketches-go/ddsketch/encoding"
)

type CollapsingHighestDenseStore struct {
	DenseStore
	MaxNumBins  int
	IsCollapsed bool
}

func NewCollapsingHighestDenseStore(maxNumBins int) *CollapsingHighestDenseStore {
	return &CollapsingHighestDenseStore{
		DenseStore:  DenseStore{MinIndex: math.MaxInt32, MaxIndex: math.MinInt32},
		MaxNumBins:  maxNumBins,
		IsCollapsed: false,
	}
}

func (s *CollapsingHighestDenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *CollapsingHighestDenseStore) AddBin(bin Bin) {
	index := bin.Index
	count := bin.Count
	if count == 0 {
		return
	}
	s.AddWithCount(index, count)
}

func (s *CollapsingHighestDenseStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	}
	arrayIndex := s.normalize(index)
	s.Bins[arrayIndex] += count
	s.Count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *CollapsingHighestDenseStore) normalize(index int) int {
	if index > s.MaxIndex {
		if s.IsCollapsed {
			return len(s.Bins) - 1
		} else {
			s.extendRange(index, index)
			if s.IsCollapsed {
				return len(s.Bins) - 1
			}
		}
	} else if index < s.MinIndex {
		s.extendRange(index, index)
	}
	return index - s.Offset
}

func (s *CollapsingHighestDenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	return min(s.DenseStore.getNewLength(newMinIndex, newMaxIndex), s.MaxNumBins)
}

func (s *CollapsingHighestDenseStore) extendRange(newMinIndex, newMaxIndex int) {
	newMinIndex = min(newMinIndex, s.MinIndex)
	newMaxIndex = max(newMaxIndex, s.MaxIndex)
	if s.IsEmpty() {
		initialLength := s.getNewLength(newMinIndex, newMaxIndex)
		s.Bins = append(s.Bins, make([]float64, initialLength)...)
		s.Offset = newMinIndex
		s.MinIndex = newMinIndex
		s.MaxIndex = newMaxIndex
		s.adjust(newMinIndex, newMaxIndex)
	} else if newMinIndex >= s.Offset && newMaxIndex < s.Offset+len(s.Bins) {
		s.MinIndex = newMinIndex
		s.MaxIndex = newMaxIndex
	} else {
		// To avoid shifting too often when nearing the capacity of the array,
		// we may grow it before we actually reach the capacity.
		newLength := s.getNewLength(newMinIndex, newMaxIndex)
		if newLength > len(s.Bins) {
			s.Bins = append(s.Bins, make([]float64, newLength-len(s.Bins))...)
		}
		s.adjust(newMinIndex, newMaxIndex)
	}
}

// Adjust Bins, offset, minIndex and maxIndex, without resizing the Bins slice in order to make it fit the
// specified range.
func (s *CollapsingHighestDenseStore) adjust(newMinIndex, newMaxIndex int) {
	if newMaxIndex-newMinIndex+1 > len(s.Bins) {
		// The range of indices is too wide, buckets of lowest indices need to be collapsed.
		newMaxIndex = newMinIndex + len(s.Bins) - 1
		if newMaxIndex <= s.MinIndex {
			// There will be only one non-empty bucket.
			s.Bins = make([]float64, len(s.Bins))
			s.Offset = newMinIndex
			s.MaxIndex = newMaxIndex
			s.Bins[len(s.Bins)-1] = s.Count
		} else {
			shift := s.Offset - newMinIndex
			if shift > 0 {
				// Collapse the buckets.
				n := float64(0)
				for i := newMaxIndex + 1; i <= s.MaxIndex; i++ {
					n += s.Bins[i-s.Offset]
				}
				s.resetBins(newMaxIndex+1, s.MaxIndex)
				s.Bins[newMaxIndex-s.Offset] += n
				s.MaxIndex = newMaxIndex
				// Shift the buckets to make room for newMinIndex.
				s.shiftCounts(shift)
			} else {
				// Shift the buckets to make room for newMaxIndex.
				s.shiftCounts(shift)
				s.MaxIndex = newMaxIndex
			}
		}
		s.MinIndex = newMinIndex
		s.IsCollapsed = true
	} else {
		s.centerCounts(newMinIndex, newMaxIndex)
	}
}

func (s *CollapsingHighestDenseStore) MergeWith(other Store) {
	if other.IsEmpty() {
		return
	}
	o, ok := other.(*CollapsingHighestDenseStore)
	if !ok {
		other.ForEach(func(index int, count float64) (stop bool) {
			s.AddWithCount(index, count)
			return false
		})
		return
	}
	if o.MinIndex < s.MinIndex || o.MaxIndex > s.MaxIndex {
		s.extendRange(o.MinIndex, o.MaxIndex)
	}
	idx := o.MaxIndex
	for ; idx > s.MaxIndex && idx >= o.MinIndex; idx-- {
		s.Bins[len(s.Bins)-1] += o.Bins[idx-o.Offset]
	}
	for ; idx > o.MinIndex; idx-- {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	// This is a separate test so that the comparison in the previous loop is strict (>) and handles
	// o.minIndex = Integer.MIN_VALUE.
	if idx == o.MinIndex {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *CollapsingHighestDenseStore) Copy() Store {
	Bins := make([]float64, len(s.Bins))
	copy(Bins, s.Bins)
	return &CollapsingHighestDenseStore{
		DenseStore: DenseStore{
			Bins:     Bins,
			Count:    s.Count,
			Offset:   s.Offset,
			MinIndex: s.MinIndex,
			MaxIndex: s.MaxIndex,
		},
		MaxNumBins:  s.MaxNumBins,
		IsCollapsed: s.IsCollapsed,
	}
}

func (s *CollapsingHighestDenseStore) Clear() {
	s.DenseStore.Clear()
	s.IsCollapsed = false
}

func (s *CollapsingHighestDenseStore) DecodeAndMergeWith(r *[]byte, encodingMode enc.SubFlag) error {
	return DecodeAndMergeWith(s, r, encodingMode)
}

var _ Store = (*CollapsingHighestDenseStore)(nil)
