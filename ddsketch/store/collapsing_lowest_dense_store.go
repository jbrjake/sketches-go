// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"math"

	enc "github.com/jbrjake/sketches-go/ddsketch/encoding"
	"github.com/jbrjake/sketches-go/ddsketch/pb/sketchpb"
)

// CollapsingLowestDenseStore is a dynamically growing contiguous (non-sparse) store.
// The lower bins get combined so that the total number of bins do not exceed maxNumBins.
type CollapsingLowestDenseStore struct {
	DenseStore
	MaxNumBins  int
	IsCollapsed bool
}

type CollapsingLowestDenseStoreProvider func() CollapsingLowestDenseStore

var CollapsingLowestDenseStoreConstructor = CollapsingLowestDenseStoreProvider(func() CollapsingLowestDenseStore { return *NewCollapsingLowestDenseStore(2048) })

func NewCollapsingLowestDenseStore(maxNumBins int) *CollapsingLowestDenseStore {
	// Bins are not allocated until values are added.
	// When the first value is added, a small number of bins are allocated. The number of bins will
	// grow as needed up to maxNumBins.
	return &CollapsingLowestDenseStore{
		DenseStore:  DenseStore{MinIndex: math.MaxInt32, MaxIndex: math.MinInt32},
		MaxNumBins:  maxNumBins,
		IsCollapsed: false,
	}
}

func (s *CollapsingLowestDenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *CollapsingLowestDenseStore) AddBin(bin Bin) {
	index := bin.Index
	count := bin.Count
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
	s.Bins[arrayIndex] += count
	s.Count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *CollapsingLowestDenseStore) normalize(index int) int {
	if index < s.MinIndex {
		if s.IsCollapsed {
			return 0
		} else {
			s.extendRange(index, index)
			if s.IsCollapsed {
				return 0
			}
		}
	} else if index > s.MaxIndex {
		s.extendRange(index, index)
	}
	return index - s.Offset
}

func (s *CollapsingLowestDenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	return min(s.DenseStore.getNewLength(newMinIndex, newMaxIndex), s.MaxNumBins)
}

func (s *CollapsingLowestDenseStore) extendRange(newMinIndex, newMaxIndex int) {
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

// Adjust bins, offset, minIndex and maxIndex, without resizing the bins slice in order to make it fit the
// specified range.
func (s *CollapsingLowestDenseStore) adjust(newMinIndex, newMaxIndex int) {
	if newMaxIndex-newMinIndex+1 > len(s.Bins) {
		// The range of indices is too wide, buckets of lowest indices need to be collapsed.
		newMinIndex = newMaxIndex - len(s.Bins) + 1
		if newMinIndex >= s.MaxIndex {
			// There will be only one non-empty bucket.
			s.Bins = make([]float64, len(s.Bins))
			s.Offset = newMinIndex
			s.MinIndex = newMinIndex
			s.Bins[0] = s.Count
		} else {
			shift := s.Offset - newMinIndex
			if shift < 0 {
				// Collapse the buckets.
				n := float64(0)
				for i := s.MinIndex; i < newMinIndex; i++ {
					n += s.Bins[i-s.Offset]
				}
				s.resetBins(s.MinIndex, newMinIndex-1)
				s.Bins[newMinIndex-s.Offset] += n
				s.MinIndex = newMinIndex
				// Shift the buckets to make room for newMaxIndex.
				s.shiftCounts(shift)
			} else {
				// Shift the buckets to make room for newMinIndex.
				s.shiftCounts(shift)
				s.MinIndex = newMinIndex
			}
		}
		s.MaxIndex = newMaxIndex
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
	if o.MinIndex < s.MinIndex || o.MaxIndex > s.MaxIndex {
		s.extendRange(o.MinIndex, o.MaxIndex)
	}
	idx := o.MinIndex
	for ; idx < s.MinIndex && idx <= o.MaxIndex; idx++ {
		s.Bins[0] += o.Bins[idx-o.Offset]
	}
	for ; idx < o.MaxIndex; idx++ {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	// This is a separate test so that the comparison in the previous loop is strict (<) and handles
	// store.MaxIndex = Integer.MAX_VALUE.
	if idx == o.MaxIndex {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *CollapsingLowestDenseStore) MergeWithCollapsingLowestDenseStore(other CollapsingLowestDenseStore) {
	if other.IsEmpty() {
		return
	}
	o := other
	if o.MinIndex < s.MinIndex || o.MaxIndex > s.MaxIndex {
		s.extendRange(o.MinIndex, o.MaxIndex)
	}
	idx := o.MinIndex
	for ; idx < s.MinIndex && idx <= o.MaxIndex; idx++ {
		s.Bins[0] += o.Bins[idx-o.Offset]
	}
	for ; idx < o.MaxIndex; idx++ {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	// This is a separate test so that the comparison in the previous loop is strict (<) and handles
	// store.MaxIndex = Integer.MAX_VALUE.
	if idx == o.MaxIndex {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *CollapsingLowestDenseStore) Copy() Store {
	bins := make([]float64, len(s.Bins))
	copy(bins, s.Bins)
	return &CollapsingLowestDenseStore{
		DenseStore: DenseStore{
			Bins:     bins,
			Count:    s.Count,
			Offset:   s.Offset,
			MinIndex: s.MinIndex,
			MaxIndex: s.MaxIndex,
		},
		MaxNumBins:  s.MaxNumBins,
		IsCollapsed: s.IsCollapsed,
	}
}

func (s *CollapsingLowestDenseStore) CopyCollapsingLowestDenseStore() *CollapsingLowestDenseStore {
	bins := make([]float64, len(s.Bins))
	copy(bins, s.Bins)
	return &CollapsingLowestDenseStore{
		DenseStore: DenseStore{
			Bins:     bins,
			Count:    s.Count,
			Offset:   s.Offset,
			MinIndex: s.MinIndex,
			MaxIndex: s.MaxIndex,
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

// MergeWithProto merges the distribution in a protobuf Store to an existing store.
// - if called with an empty store, this simply populates the store with the distribution in the protobuf Store.
// - if called with a non-empty store, this has the same outcome as deserializing the protobuf Store, then merging.
func MergeCollapsingLowestDenseStoreWithProto(store CollapsingLowestDenseStore, pb *sketchpb.Store) {
	for idx, count := range pb.BinCounts {
		store.AddWithCount(int(idx), count)
	}
	for idx, count := range pb.ContiguousBinCounts {
		store.AddWithCount(idx+int(pb.ContiguousBinIndexOffset), count)
	}
}
