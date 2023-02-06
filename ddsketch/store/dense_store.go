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
	Bins     []float64
	Count    float64
	Offset   int
	MinIndex int
	MaxIndex int
}

func NewDenseStore() *DenseStore {
	return &DenseStore{MinIndex: math.MaxInt32, MaxIndex: math.MinInt32}
}

func (s *DenseStore) Add(index int) {
	s.AddWithCount(index, float64(1))
}

func (s *DenseStore) AddBin(bin Bin) {
	if bin.Count == 0 {
		return
	}
	s.AddWithCount(bin.Index, bin.Count)
}

func (s *DenseStore) AddWithCount(index int, count float64) {
	if count == 0 {
		return
	}
	arrayIndex := s.normalize(index)
	s.Bins[arrayIndex] += count
	s.Count += count
}

// Normalize the store, if necessary, so that the counter of the specified index can be updated.
func (s *DenseStore) normalize(index int) int {
	if index < s.MinIndex || index > s.MaxIndex {
		s.extendRange(index, index)
	}
	return index - s.Offset
}

func (s *DenseStore) getNewLength(newMinIndex, newMaxIndex int) int {
	desiredLength := newMaxIndex - newMinIndex + 1
	return int((float64(desiredLength+arrayLengthOverhead-1)/arrayLengthGrowthIncrement + 1) * arrayLengthGrowthIncrement)
}

func (s *DenseStore) extendRange(newMinIndex, newMaxIndex int) {

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
func (s *DenseStore) adjust(newMinIndex, newMaxIndex int) {
	s.centerCounts(newMinIndex, newMaxIndex)
}

func (s *DenseStore) centerCounts(newMinIndex, newMaxIndex int) {
	midIndex := newMinIndex + (newMaxIndex-newMinIndex+1)/2
	s.shiftCounts(s.Offset + len(s.Bins)/2 - midIndex)
	s.MinIndex = newMinIndex
	s.MaxIndex = newMaxIndex
}

func (s *DenseStore) shiftCounts(shift int) {
	minArrIndex := s.MinIndex - s.Offset
	maxArrIndex := s.MaxIndex - s.Offset
	copy(s.Bins[minArrIndex+shift:], s.Bins[minArrIndex:maxArrIndex+1])
	if shift > 0 {
		s.resetBins(s.MinIndex, s.MinIndex+shift-1)
	} else {
		s.resetBins(s.MaxIndex+shift+1, s.MaxIndex)
	}
	s.Offset -= shift
}

func (s *DenseStore) resetBins(fromIndex, toIndex int) {
	for i := fromIndex - s.Offset; i <= toIndex-s.Offset; i++ {
		s.Bins[i] = 0
	}
}

func (s *DenseStore) IsEmpty() bool {
	return s.Count == 0
}

func (s *DenseStore) TotalCount() float64 {
	return s.Count
}

func (s *DenseStore) GetMinIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errUndefinedMinIndex
	}
	return s.MinIndex, nil
}

func (s *DenseStore) GetMaxIndex() (int, error) {
	if s.IsEmpty() {
		return 0, errUndefinedMaxIndex
	}
	return s.MaxIndex, nil
}

// Return the key for the value at rank
func (s *DenseStore) KeyAtRank(rank float64) int {
	if rank < 0 {
		rank = 0
	}
	var n float64
	for i, b := range s.Bins {
		n += b
		if n > rank {
			return i + s.Offset
		}
	}
	return s.MaxIndex
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
	if o.MinIndex < s.MinIndex || o.MaxIndex > s.MaxIndex {
		s.extendRange(o.MinIndex, o.MaxIndex)
	}
	for idx := o.MinIndex; idx <= o.MaxIndex; idx++ {
		s.Bins[idx-s.Offset] += o.Bins[idx-o.Offset]
	}
	s.Count += o.Count
}

func (s *DenseStore) GetBins() <-chan Bin {
	ch := make(chan Bin)
	go func() {
		defer close(ch)
		for idx := s.MinIndex; idx <= s.MaxIndex; idx++ {
			if s.Bins[idx-s.Offset] > 0 {
				ch <- Bin{Index: idx, Count: s.Bins[idx-s.Offset]}
			}
		}
	}()
	return ch
}

func (s *DenseStore) ForEach(f func(index int, count float64) (stop bool)) {
	for idx := s.MinIndex; idx <= s.MaxIndex; idx++ {
		if s.Bins[idx-s.Offset] > 0 {
			if f(idx, s.Bins[idx-s.Offset]) {
				return
			}
		}
	}
}

func (s *DenseStore) Copy() Store {
	bins := make([]float64, len(s.Bins))
	copy(bins, s.Bins)
	return &DenseStore{
		Bins:     bins,
		Count:    s.Count,
		Offset:   s.Offset,
		MinIndex: s.MinIndex,
		MaxIndex: s.MaxIndex,
	}
}

func (s *DenseStore) Clear() {
	s.Bins = s.Bins[:0]
	s.Count = 0
	s.MinIndex = math.MaxInt32
	s.MaxIndex = math.MinInt32
}

func (s *DenseStore) string() string {
	var buffer bytes.Buffer
	buffer.WriteString("{")
	for i := 0; i < len(s.Bins); i++ {
		index := i + s.Offset
		buffer.WriteString(fmt.Sprintf("%d: %f, ", index, s.Bins[i]))
	}
	buffer.WriteString(fmt.Sprintf("count: %v, offset: %d, minIndex: %d, maxIndex: %d}", s.Count, s.Offset, s.MinIndex, s.MaxIndex))
	return buffer.String()
}

func (s *DenseStore) ToProto() *sketchpb.Store {
	if s.IsEmpty() {
		return &sketchpb.Store{ContiguousBinCounts: nil}
	}
	bins := make([]float64, s.MaxIndex-s.MinIndex+1)
	copy(bins, s.Bins[s.MinIndex-s.Offset:s.MaxIndex-s.Offset+1])
	return &sketchpb.Store{
		ContiguousBinCounts:      bins,
		ContiguousBinIndexOffset: int32(s.MinIndex),
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
	for idx := s.MinIndex; idx <= s.MaxIndex; idx++ {
		s.Bins[idx-s.Offset] *= w
	}
	return nil
}

func (s *DenseStore) Encode(b *[]byte, t enc.FlagType) {
	if s.IsEmpty() {
		return
	}

	denseEncodingSize := 0
	numBins := uint64(s.MaxIndex-s.MinIndex) + 1
	denseEncodingSize += enc.Uvarint64Size(numBins)
	denseEncodingSize += enc.Varint64Size(int64(s.MinIndex))
	denseEncodingSize += enc.Varint64Size(1)

	sparseEncodingSize := 0
	numNonEmptyBins := uint64(0)

	previousIndex := s.MinIndex
	for index := s.MinIndex; index <= s.MaxIndex; index++ {
		count := s.Bins[index-s.Offset]
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
	enc.EncodeVarint64(b, int64(s.MinIndex))
	enc.EncodeVarint64(b, 1)
	for index := s.MinIndex; index <= s.MaxIndex; index++ {
		enc.EncodeVarfloat64(b, s.Bins[index-s.Offset])
	}
}

func (s *DenseStore) encodeSparsely(b *[]byte, t enc.FlagType, numNonEmptyBins uint64) {
	enc.EncodeFlag(b, enc.NewFlag(t, enc.BinEncodingIndexDeltasAndCounts))
	enc.EncodeUvarint64(b, numNonEmptyBins)
	previousIndex := 0
	for index := s.MinIndex; index <= s.MaxIndex; index++ {
		count := s.Bins[index-s.Offset]
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
