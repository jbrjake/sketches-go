// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2021 Datadog, Inc.

package logCollapsingLowestDdsketch

import (
	"errors"
	"math"

	"github.com/jbrjake/sketches-go/ddsketch/mapping"
	"github.com/jbrjake/sketches-go/ddsketch/pb/sketchpb"
	"github.com/jbrjake/sketches-go/ddsketch/store"
)

var (
	ErrUntrackableNaN     = errors.New("input value is NaN and cannot be tracked by the sketch")
	ErrUntrackableTooLow  = errors.New("input value is too low and cannot be tracked by the sketch")
	ErrUntrackableTooHigh = errors.New("input value is too high and cannot be tracked by the sketch")
	ErrNegativeCount      = errors.New("count cannot be negative")
	errEmptySketch        = errors.New("no such element exists")
)

type LogCollapsingLowestDenseDDSketch struct {
	IndexMapping       mapping.LogarithmicMapping
	PositiveValueStore store.CollapsingLowestDenseStore
	NegativeValueStore store.CollapsingLowestDenseStore
	ZeroCount          float64
}

func NewLogCollapsingLowestDenseDDSketch(relativeAccuracy float64, maxNumBins int) (*LogCollapsingLowestDenseDDSketch, error) {
	indexMapping, err := mapping.NewLogarithmicMapping(relativeAccuracy)
	if err != nil {
		return nil, err
	}
	return NewSpecifiedLogCollapsingLowestDenseDDSketch(*indexMapping, *store.NewCollapsingLowestDenseStore(maxNumBins), *store.NewCollapsingLowestDenseStore(maxNumBins)), nil
}

func NewSpecifiedLogCollapsingLowestDenseDDSketch(indexMapping mapping.LogarithmicMapping, positiveValueStore store.CollapsingLowestDenseStore, negativeValueStore store.CollapsingLowestDenseStore) *LogCollapsingLowestDenseDDSketch {
	return &LogCollapsingLowestDenseDDSketch{
		IndexMapping:       indexMapping,
		PositiveValueStore: positiveValueStore,
		NegativeValueStore: negativeValueStore,
	}
}

// Adds a value to the sketch.
func (s *LogCollapsingLowestDenseDDSketch) Add(value float64) error {
	return s.AddWithCount(value, float64(1))
}

// Adds a value to the sketch with a float64 count.
func (s *LogCollapsingLowestDenseDDSketch) AddWithCount(value, count float64) error {
	if count < 0 {
		return ErrNegativeCount
	}

	if value > s.IndexMapping.GetMinIndexableValue() {
		if value > s.IndexMapping.GetMaxIndexableValue() {
			return ErrUntrackableTooHigh
		}
		s.PositiveValueStore.AddWithCount(s.IndexMapping.Index(value), count)
	} else if value < -s.IndexMapping.GetMinIndexableValue() {
		if value < -s.IndexMapping.GetMaxIndexableValue() {
			return ErrUntrackableTooLow
		}
		s.NegativeValueStore.AddWithCount(s.IndexMapping.Index(-value), count)
	} else if math.IsNaN(value) {
		return ErrUntrackableNaN
	} else {
		s.ZeroCount += count
	}
	return nil
}

// Return a (deep) copy of this sketch.
func (s *LogCollapsingLowestDenseDDSketch) Copy() *LogCollapsingLowestDenseDDSketch {
	return &LogCollapsingLowestDenseDDSketch{
		IndexMapping:       s.IndexMapping,
		PositiveValueStore: *s.PositiveValueStore.CopyCollapsingLowestDenseStore(),
		NegativeValueStore: *s.NegativeValueStore.CopyCollapsingLowestDenseStore(),
		ZeroCount:          s.ZeroCount,
	}
}

// Clear empties the sketch while allowing reusing already allocated memory.
func (s *LogCollapsingLowestDenseDDSketch) Clear() {
	s.PositiveValueStore.Clear()
	s.NegativeValueStore.Clear()
	s.ZeroCount = 0
}

// Return the value at the specified quantile. Return a non-nil error if the quantile is invalid
// or if the sketch is empty.
func (s *LogCollapsingLowestDenseDDSketch) GetValueAtQuantile(quantile float64) (float64, error) {
	if quantile < 0 || quantile > 1 {
		return math.NaN(), errors.New("The quantile must be between 0 and 1.")
	}

	count := s.GetCount()
	if count == 0 {
		return math.NaN(), errEmptySketch
	}

	rank := quantile * (count - 1)
	negativeValueCount := s.NegativeValueStore.TotalCount()
	if rank < negativeValueCount {
		return -s.IndexMapping.Value(s.NegativeValueStore.KeyAtRank(negativeValueCount - 1 - rank)), nil
	} else if rank < s.ZeroCount+negativeValueCount {
		return 0, nil
	} else {
		return s.IndexMapping.Value(s.PositiveValueStore.KeyAtRank(rank - s.ZeroCount - negativeValueCount)), nil
	}
}

// Return the values at the respective specified quantiles. Return a non-nil error if any of the quantiles
// is invalid or if the sketch is empty.
func (s *LogCollapsingLowestDenseDDSketch) GetValuesAtQuantiles(quantiles []float64) ([]float64, error) {
	values := make([]float64, len(quantiles))
	for i, q := range quantiles {
		val, err := s.GetValueAtQuantile(q)
		if err != nil {
			return nil, err
		}
		values[i] = val
	}
	return values, nil
}

// Return the total number of values that have been added to this sketch.
func (s *LogCollapsingLowestDenseDDSketch) GetCount() float64 {
	return s.ZeroCount + s.PositiveValueStore.TotalCount() + s.NegativeValueStore.TotalCount()
}

// GetZeroCount returns the number of zero values that have been added to this sketch.
// Note: values that are very small (lower than MinIndexableValue if positive, or higher than -MinIndexableValue if negative)
// are also mapped to the zero bucket.
func (s *LogCollapsingLowestDenseDDSketch) GetZeroCount() float64 {
	return s.ZeroCount
}

// Return true iff no value has been added to this sketch.
func (s *LogCollapsingLowestDenseDDSketch) IsEmpty() bool {
	return s.ZeroCount == 0 && s.PositiveValueStore.IsEmpty() && s.NegativeValueStore.IsEmpty()
}

// Return the maximum value that has been added to this sketch. Return a non-nil error if the sketch
// is empty.
func (s *LogCollapsingLowestDenseDDSketch) GetMaxValue() (float64, error) {
	if !s.PositiveValueStore.IsEmpty() {
		maxIndex, _ := s.PositiveValueStore.GetMaxIndex()
		return s.IndexMapping.Value(maxIndex), nil
	} else if s.ZeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.NegativeValueStore.GetMinIndex()
		if err != nil {
			return math.NaN(), err
		}
		return -s.IndexMapping.Value(minIndex), nil
	}
}

// Return the minimum value that has been added to this sketch. Returns a non-nil error if the sketch
// is empty.
func (s *LogCollapsingLowestDenseDDSketch) GetMinValue() (float64, error) {
	if !s.NegativeValueStore.IsEmpty() {
		maxIndex, _ := s.NegativeValueStore.GetMaxIndex()
		return -s.IndexMapping.Value(maxIndex), nil
	} else if s.ZeroCount > 0 {
		return 0, nil
	} else {
		minIndex, err := s.PositiveValueStore.GetMinIndex()
		if err != nil {
			return math.NaN(), err
		}
		return s.IndexMapping.Value(minIndex), nil
	}
}

// GetSum returns an approximation of the sum of the values that have been added to the sketch. If the
// values that have been added to the sketch all have the same sign, the approximation error has
// the relative accuracy guarantees of the mapping used for this sketch.
func (s *LogCollapsingLowestDenseDDSketch) GetSum() (sum float64) {
	s.ForEach(func(value float64, count float64) (stop bool) {
		sum += value * count
		return false
	})
	return sum
}

// GetPositiveValueStore returns the store.Store object that contains the positive
// values of the sketch.
func (s *LogCollapsingLowestDenseDDSketch) GetPositiveValueStore() store.CollapsingLowestDenseStore {
	return s.PositiveValueStore
}

// GetNegativeValueStore returns the store.Store object that contains the negative
// values of the sketch.
func (s *LogCollapsingLowestDenseDDSketch) GetNegativeValueStore() store.CollapsingLowestDenseStore {
	return s.NegativeValueStore
}

// ForEach applies f on the bins of the sketches until f returns true.
// There is no guarantee on the bin iteration order.
func (s *LogCollapsingLowestDenseDDSketch) ForEach(f func(value, count float64) (stop bool)) {
	if s.ZeroCount != 0 && f(0, s.ZeroCount) {
		return
	}
	stopped := false
	s.PositiveValueStore.ForEach(func(index int, count float64) bool {
		stopped = f(s.IndexMapping.Value(index), count)
		return stopped
	})
	if stopped {
		return
	}
	s.NegativeValueStore.ForEach(func(index int, count float64) bool {
		return f(-s.IndexMapping.Value(index), count)
	})
}

// Merges the other sketch into this one. After this operation, this sketch encodes the values that
// were added to both this and the other sketches.
func (s *LogCollapsingLowestDenseDDSketch) MergeWith(other *LogCollapsingLowestDenseDDSketch) error {
	s.PositiveValueStore.MergeWithCollapsingLowestDenseStore(other.PositiveValueStore)
	s.NegativeValueStore.MergeWithCollapsingLowestDenseStore(other.NegativeValueStore)
	s.ZeroCount += other.ZeroCount
	return nil
}

// Generates a protobuf representation of this DDSketch.
func (s *LogCollapsingLowestDenseDDSketch) ToProto() *sketchpb.DDSketch {
	return &sketchpb.DDSketch{
		Mapping:        s.IndexMapping.ToProto(),
		PositiveValues: s.PositiveValueStore.ToProto(),
		NegativeValues: s.NegativeValueStore.ToProto(),
		ZeroCount:      s.ZeroCount,
	}
}

// FromProto builds a new instance of DDSketch based on the provided protobuf representation, using a Dense store.
func FromProto(pb *sketchpb.DDSketch) (*LogCollapsingLowestDenseDDSketch, error) {
	return FromProtoWithStoreProvider(pb, store.CollapsingLowestDenseStoreConstructor)
}

func FromProtoWithStoreProvider(pb *sketchpb.DDSketch, storeProvider store.CollapsingLowestDenseStoreProvider) (*LogCollapsingLowestDenseDDSketch, error) {
	positiveValueStore := storeProvider()
	store.MergeCollapsingLowestDenseStoreWithProto(positiveValueStore, pb.PositiveValues)
	negativeValueStore := storeProvider()
	store.MergeCollapsingLowestDenseStoreWithProto(negativeValueStore, pb.NegativeValues)
	logM, err := mapping.NewLogarithmicMappingWithGamma(pb.Mapping.Gamma, pb.Mapping.IndexOffset)
	if err != nil {
		return nil, err
	}
	return &LogCollapsingLowestDenseDDSketch{
		IndexMapping:       *logM,
		PositiveValueStore: *positiveValueStore,
		NegativeValueStore: *negativeValueStore,
		ZeroCount:          pb.ZeroCount,
	}, nil
}

// Reweight multiplies all values from the sketch by w, but keeps the same global distribution.
// w has to be strictly greater than 0.
func (s *LogCollapsingLowestDenseDDSketch) Reweight(w float64) error {
	if w <= 0 {
		return errors.New("can't reweight by a negative factor")
	}
	if w == 1 {
		return nil
	}
	s.ZeroCount *= w
	if err := s.PositiveValueStore.Reweight(w); err != nil {
		return err
	}
	if err := s.NegativeValueStore.Reweight(w); err != nil {
		return err
	}
	return nil
}
