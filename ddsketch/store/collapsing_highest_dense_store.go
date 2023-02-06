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
		DenseStore:  DenseStore{_MinIndex: math.MaxInt32, _MaxIndex: math.MinInt32},
		MaxNumBins:  maxNumBins,
		IsCollapsed: false,
	}
}

func (s *CollapsingHighestDenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *CollapsingHighestDenseStore) AddBin(bin Bin) {
	index := bin.Index()
	count := bin.Count()
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
	s._Bins[arrayIndex] += count
	s.Count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *CollapsingHighestDenseStore) normalize(index int) int {
	if index > s._MaxIndex {
		if s.IsCollapsed {
			return len(s._Bins) - 1
		} else {
			s.extendRange(index, index)
			if s.IsCollapsed {
				return len(s._Bins) - 1
			}
		}
	} else if index < s._MinIndex {
		s.extendRange(index, index)
	}
	return index - s.Offset
}

func (s *CollapsingHighestDenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	return min(s.DenseStore.getNewLength(newMinIndex, newMaxIndex), s.MaxNumBins)
}

func (s *CollapsingHighestDenseStore) extendRange(newMinIndex, newMaxIndex int) {
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

// Adjust _Bins, offset, minIndex and maxIndex, without resizing the _Bins slice in order to make it fit the
// specified range.
func (s *CollapsingHighestDenseStore) adjust(newMinIndex, newMaxIndex int) {
	if newMaxIndex-newMinIndex+1 > len(s._Bins) {
		// The range of indices is too wide, buckets of lowest indices need to be collapsed.
		newMaxIndex = newMinIndex + len(s._Bins) - 1
		if newMaxIndex <= s._MinIndex {
			// There will be only one non-empty bucket.
			s._Bins = make([]float64, len(s._Bins))
			s.Offset = newMinIndex
			s._MaxIndex = newMaxIndex
			s._Bins[len(s._Bins)-1] = s.Count
		} else {
			shift := s.Offset - newMinIndex
			if shift > 0 {
				// Collapse the buckets.
				n := float64(0)
				for i := newMaxIndex + 1; i <= s._MaxIndex; i++ {
					n += s._Bins[i-s.Offset]
				}
				s.resetBins(newMaxIndex+1, s._MaxIndex)
				s._Bins[newMaxIndex-s.Offset] += n
				s._MaxIndex = newMaxIndex
				// Shift the buckets to make room for newMinIndex.
				s.shiftCounts(shift)
			} else {
				// Shift the buckets to make room for newMaxIndex.
				s.shiftCounts(shift)
				s._MaxIndex = newMaxIndex
			}
		}
		s._MinIndex = newMinIndex
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
	if o._MinIndex < s._MinIndex || o._MaxIndex > s._MaxIndex {
		s.extendRange(o._MinIndex, o._MaxIndex)
	}
	idx := o._MaxIndex
	for ; idx > s._MaxIndex && idx >= o._MinIndex; idx-- {
		s._Bins[len(s._Bins)-1] += o._Bins[idx-o.Offset]
	}
	for ; idx > o._MinIndex; idx-- {
		s._Bins[idx-s.Offset] += o._Bins[idx-o.Offset]
	}
	// This is a separate test so that the comparison in the previous loop is strict (>) and handles
	// o.minIndex = Integer.MIN_VALUE.
	if idx == o._MinIndex {
		s._Bins[idx-s.Offset] += o._Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *CollapsingHighestDenseStore) Copy() Store {
	_Bins := make([]float64, len(s._Bins))
	copy(_Bins, s._Bins)
	return &CollapsingHighestDenseStore{
		DenseStore: DenseStore{
			_Bins:     _Bins,
			Count:     s.Count,
			Offset:    s.Offset,
			_MinIndex: s._MinIndex,
			_MaxIndex: s._MaxIndex,
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
