// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package store

import (
	"bytes"
	"errors"
	"fmt"
	"math"

	enc "github.com/jbrjake/sketches-go/ddsketch/encoding"
	"github.com/jbrjake/sketches-go/ddsketch/pb/sketchpb"
)

const (
	arrayLengthOverhead        = 64
	arrayLengthGrowthIncrement = 0.1

	// Grow the bins with an extra growthBuffer bins to prevent growing too often
	growthBuffer = 128
)

// DenseStore is a dynamically growing contiguous (non-sparse) store. The number of bins are
// bound only by the size of the slice that can be allocated.
type DenseStore struct {
	_Bins     []float64
	Count     float64
	Offset    int
	_MinIndex int
	_MaxIndex int
}

func NewDenseStore() *DenseStore {
	return &DenseStore{_MinIndex: math.MaxInt32, _MaxIndex: math.MinInt32}
}

func (s *DenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *DenseStore) AddBin(bin Bin) {
	if bin._Count == 0 {
		return
	}
	s.AddWithCount(bin._Index, bin._Count)
}

func (s *DenseStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	}
	arrayIndex := s.normalize(index)
	s._Bins[arrayIndex] += count
	s.Count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *DenseStore) normalize(index int) int {
	if index < s._MinIndex || index > s._MaxIndex {
		s.extendRange(index, index)
	}
	return index - s.Offset
}

func (s *DenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	desiredLength := newMaxIndex - newMinIndex + 1
	return int((float64(desiredLength+arrayLengthOverhead-1)/arrayLengthGrowthIncrement + 1) * arrayLengthGrowthIncrement)
}

func (s *DenseStore) extendRange(newMinIndex, newMaxIndex int) {

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
func (s *DenseStore) adjust(newMinIndex, newMaxIndex int) {
	s.centerCounts(newMinIndex, newMaxIndex)
}

func (s *DenseStore) centerCounts(newMinIndex, newMaxIndex int) {
	midIndex := newMinIndex + (newMaxIndex-newMinIndex+1)/2
	s.shiftCounts(s.Offset + len(s._Bins)/2 - midIndex)
	s._MinIndex = newMinIndex
	s._MaxIndex = newMaxIndex
}

func (s *DenseStore) shiftCounts(shift int) {
	minArrIndex := s._MinIndex - s.Offset
	maxArrIndex := s._MaxIndex - s.Offset
	copy(s._Bins[minArrIndex+shift:], s._Bins[minArrIndex:maxArrIndex+1])
	if shift > 0 {
		s.resetBins(s._MinIndex, s._MinIndex+shift-1)
	} else {
		s.resetBins(s._MaxIndex+shift+1, s._MaxIndex)
	}
	s.Offset -= shift
}

func (s *DenseStore) resetBins(fromIndex, toIndex int) {
	for i := fromIndex - s.Offset; i <= toIndex-s.Offset; i++ {
		s._Bins[i] = 0
	}
}

func (s *DenseStore) IsEmpty() bool {
	return s.Count == 0
}

func (s *DenseStore) TotalCount() float64 {
	return s.Count
}

func (s *DenseStore) MinIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errUndefinedMinIndex
	}
	return s._MinIndex, nil
}

func (s *DenseStore) MaxIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errUndefinedMaxIndex
	}
	return s._MaxIndex, nil
}

// Return the key for the value at rank
func (s *DenseStore) KeyAtRank(rank float64) int {
	if rank < 0 {
		rank = 0
	}
	var n float64
	for i, b := range s._Bins {
		n += b
		if n > rank {
			return i + s.Offset
		}
	}
	return s._MaxIndex
}

func (s *DenseStore) MergeWith(other Store) {
	if other.IsEmpty() {
		return
	}
	o, ok := other.(*DenseStore)
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
	for idx := o._MinIndex; idx <= o._MaxIndex; idx++ {
		s._Bins[idx-s.Offset] += o._Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *DenseStore) Bins() <-chan Bin {
	ch := make(chan Bin)
	go func() {
		defer close(ch)
		for idx := s._MinIndex; idx <= s._MaxIndex; idx++ {
			if s._Bins[idx-s.Offset] > 0 {
				ch <- Bin{_Index: idx, _Count: s._Bins[idx-s.Offset]}
			}
		}
	}()
	return ch
}

func (s *DenseStore) ForEach(f func(index int, count float64) (stop bool)) {
	for idx := s._MinIndex; idx <= s._MaxIndex; idx++ {
		if s._Bins[idx-s.Offset] > 0 {
			if f(idx, s._Bins[idx-s.Offset]) {
				return
			}
		}
	}
}

func (s *DenseStore) Copy() Store {
	bins := make([]float64, len(s._Bins))
	copy(bins, s._Bins)
	return &DenseStore{
		_Bins:     bins,
		Count:     s.Count,
		Offset:    s.Offset,
		_MinIndex: s._MinIndex,
		_MaxIndex: s._MaxIndex,
	}
}

func (s *DenseStore) Clear() {
	s._Bins = s._Bins[:0]
	s.Count = 0
	s._MinIndex = math.MaxInt32
	s._MaxIndex = math.MinInt32
}

func (s *DenseStore) string() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := 0; i < len(s._Bins); i++ {
		index := i + s.Offset
		buffer.WriteString(fmt.Sprintf("%d: %f, ", index, s._Bins[i]))
	}
	buffer.WriteString(fmt.Sprintf("count: %v, offset: %d, minIndex: %d, maxIndex: %d}", s.Count, s.Offset, s._MinIndex, s._MaxIndex))
	return buffer.String()
}

func (s *DenseStore) ToProto() *sketchpb.Store {
	if s.IsEmpty() {
		return &sketchpb.Store{ContiguousBinCounts: nil}
	}
	bins := make([]float64, s._MaxIndex-s._MinIndex+1)
	copy(bins, s._Bins[s._MinIndex-s.Offset:s._MaxIndex-s.Offset+1])
	return &sketchpb.Store{
		ContiguousBinCounts:      bins,
		ContiguousBinIndexOffset: int32(s._MinIndex),
	}
}

func (s *DenseStore) Reweight(w float64) error {
	if w <= 0 {
		return errors.New("can't reweight by a negative factor")
	}
	if w == 1 {
		return nil
	}
	s.Count *= w
	for idx := s._MinIndex; idx <= s._MaxIndex; idx++ {
		s._Bins[idx-s.Offset] *= w
	}
	return nil
}

func (s *DenseStore) Encode(b *[]byte, t enc.FlagType) {
	if s.IsEmpty() {
		return
	}

	denseEncodingSize := 0
	numBins := uint64(s._MaxIndex-s._MinIndex) + 1
	denseEncodingSize += enc.Uvarint64Size(numBins)
	denseEncodingSize += enc.Varint64Size(int64(s._MinIndex))
	denseEncodingSize += enc.Varint64Size(1)

	sparseEncodingSize := 0
	numNonEmptyBins := uint64(0)

	previousIndex := s._MinIndex
	for index := s._MinIndex; index <= s._MaxIndex; index++ {
		count := s._Bins[index-s.Offset]
		countVarFloat64Size := enc.Varfloat64Size(count)
		denseEncodingSize += countVarFloat64Size
		if count != 0 {
			numNonEmptyBins++
			sparseEncodingSize += enc.Varint64Size(int64(index - previousIndex))
			sparseEncodingSize += countVarFloat64Size
			previousIndex = index
		}
	}
	sparseEncodingSize += enc.Uvarint64Size(numNonEmptyBins)

	if denseEncodingSize <= sparseEncodingSize {
		s.encodeDensely(b, t, numBins)
	} else {
		s.encodeSparsely(b, t, numNonEmptyBins)
	}
}

func (s *DenseStore) encodeDensely(b *[]byte, t enc.FlagType, numBins uint64) {
	enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingContiguousCounts))
	enc.EncodeUvarint64(b, numBins)
	enc.EncodeVarint64(b, int64(s._MinIndex))
	enc.EncodeVarint64(b, 1)
	for index := s._MinIndex; index <= s._MaxIndex; index++ {
		enc.EncodeVarfloat64(b, s._Bins[index-s.Offset])
	}
}

func (s *DenseStore) encodeSparsely(b *[]byte, t enc.FlagType, numNonEmptyBins uint64) {
	enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingIndexDeltasAndCounts))
	enc.EncodeUvarint64(b, numNonEmptyBins)
	previousIndex := 0
	for index := s._MinIndex; index <= s._MaxIndex; index++ {
		count := s._Bins[index-s.Offset]
		if count != 0 {
			enc.EncodeVarint64(b, int64(index-previousIndex))
			enc.EncodeVarfloat64(b, count)
			previousIndex = index
		}
	}
}

func (s *DenseStore) DecodeAndMergeWith(b *[]byte, encodingMode enc.SubFlag) error {
	return DecodeAndMergeWith(s, b, encodingMode)
}

var _ Store = (*DenseStore)(nil)
