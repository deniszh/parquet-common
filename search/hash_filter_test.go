// Copyright The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package search

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	prom_storage "github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type testHashFilter struct {
	shard, count                   uint64
	labelsCalls, hashCalls, closes int
}

func (f *testHashFilter) Filter(ls labels.Labels) bool {
	f.labelsCalls++
	return labels.StableHash(ls)%f.count == f.shard
}
func (f *testHashFilter) FilterHash(h uint64) bool { f.hashCalls++; return h%f.count == f.shard }
func (f *testHashFilter) Close()                   { f.closes++ }

var _ MaterializedSeriesHashFilter = (*testHashFilter)(nil)

type testLabelsOnlyFilter struct{ MaterializedLabelsFilter }

func hashFilterFixture(t testing.TB, width int) (storage.ParquetShard, util.TestData) {
	t.Helper()
	st := teststorage.New(t)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	ctx := context.Background()
	data := util.GenerateTestData(t, st, ctx, util.TestConfig{TotalMetricNames: 1, MetricsPerMetricName: 128, NumberOfLabels: width, RandomLabels: 1, NumberOfSamples: 8})
	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })
	shard := convertToParquet(t, ctx, bkt, data, st.Head())
	t.Cleanup(func() { require.NoError(t, shard.(interface{ Close() error }).Close()) })
	return shard, data
}

func hashFilterMaterializer(t testing.TB, shard storage.ParquetShard, f MaterializedLabelsFilter, projection bool) *Materializer {
	t.Helper()
	s, err := shard.TSDBSchema()
	require.NoError(t, err)
	m, err := NewMaterializer(s, schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool()), shard, 4, UnlimitedQuota(), UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, func(context.Context, *prom_storage.SelectHints) (MaterializedLabelsFilter, bool) { return f, true }, projection)
	require.NoError(t, err)
	return m
}

func collectHashFilterResults(t testing.TB, set ChunkSeriesSetCloser) map[string][]string {
	t.Helper()
	defer func() { require.NoError(t, set.Close()) }()
	out := map[string][]string{}
	for set.Next() {
		series := set.At()
		key := series.Labels().String()
		out[key] = nil
		ci := series.Iterator(nil)
		for ci.Next() {
			it := ci.At().Chunk.Iterator(nil)
			for it.Next() != chunkenc.ValNone {
				ts, v := it.At()
				out[key] = append(out[key], fmt.Sprintf("%d:%g", ts, v))
			}
			require.NoError(t, it.Err())
		}
		require.NoError(t, ci.Err())
	}
	require.NoError(t, set.Err())
	return out
}

func TestMaterializeEarlyHashFilter(t *testing.T) {
	shard, data := hashFilterFixture(t, 8)
	for _, skipChunks := range []bool{false, true} {
		for _, ranges := range [][]RowRange{{{From: 0, Count: 128}}, {{From: 1, Count: 8}, {From: 50, Count: 5}}, nil} {
			for idx := range uint64(4) {
				rr := append([]RowRange(nil), ranges...)
				early := &testHashFilter{shard: idx, count: 4}
				late := &testHashFilter{shard: idx, count: 4}
				fast := hashFilterMaterializer(t, shard, early, false)
				oracle := hashFilterMaterializer(t, shard, testLabelsOnlyFilter{late}, false)
				expected, err := oracle.Materialize(context.Background(), nil, 0, data.MinTime, data.MaxTime, skipChunks, rr)
				require.NoError(t, err)
				actual, err := fast.Materialize(context.Background(), nil, 0, data.MinTime, data.MaxTime, skipChunks, rr)
				require.NoError(t, err)
				require.Equal(t, collectHashFilterResults(t, expected), collectHashFilterResults(t, actual))
				require.Equal(t, ranges, rr, "constraint-cache ranges must not change")
				require.Zero(t, early.labelsCalls)
				require.Equal(t, int(totalRows(rr)), early.hashCalls)
				require.Equal(t, 1, early.closes)
				require.Equal(t, 1, late.closes)
			}
		}
	}
	t.Run("cancelled", func(t *testing.T) {
		f := &testHashFilter{count: 4}
		m := hashFilterMaterializer(t, shard, f, false)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := m.Materialize(ctx, nil, 0, data.MinTime, data.MaxTime, true, []RowRange{{From: 0, Count: 128}})
		require.ErrorContains(t, err, "context canceled")
		require.Equal(t, 1, f.closes)
	})
	t.Run("projection retains full identity", func(t *testing.T) {
		f := &testHashFilter{count: 4}
		m := hashFilterMaterializer(t, shard, f, true)
		hints := &prom_storage.SelectHints{ProjectionInclude: true, ProjectionLabels: []string{"unique"}}
		result, err := m.Materialize(context.Background(), hints, 0, data.MinTime, data.MaxTime, true, []RowRange{{From: 0, Count: 128}})
		require.NoError(t, err)
		actual := collectHashFilterResults(t, result)
		oracle := hashFilterMaterializer(t, shard, &testHashFilter{count: 4}, false)
		full, err := oracle.Materialize(context.Background(), nil, 0, data.MinTime, data.MaxTime, true, []RowRange{{From: 0, Count: 128}})
		require.NoError(t, err)
		expected := map[string][]string{}
		for full.Next() {
			expected[labels.FromStrings("unique", full.At().Labels().Get("unique")).String()] = nil
		}
		require.NoError(t, full.Err())
		require.NoError(t, full.Close())
		require.Equal(t, expected, actual)
	})
}

type noHashFile struct{ storage.ParquetFileView }

func (f noHashFile) Schema() *parquet.Schema {
	group := parquet.Group{}
	for _, field := range f.ParquetFileView.Schema().Fields() {
		if field.Name() != schema.SeriesHashColumn {
			group[field.Name()] = field
		}
	}
	return parquet.NewSchema("old", group)
}

type noHashShard struct{ storage.ParquetShard }

func (s noHashShard) LabelsFile() storage.ParquetFileView {
	return noHashFile{s.ParquetShard.LabelsFile()}
}
func TestEarlyHashFilterMissingHash(t *testing.T) {
	shard, _ := hashFilterFixture(t, 1)
	m := &Materializer{b: noHashShard{shard}}
	rr := []RowRange{{From: 5, Count: 10}}
	actual, _, filtered, err := m.filterBySeriesHash(context.Background(), 0, rr, func(uint64) bool { t.Fatal("hash predicate called"); return false })
	require.NoError(t, err)
	require.False(t, filtered)
	require.Equal(t, rr, actual)
}

func TestMaterializeMissingHashFallsBackToLabels(t *testing.T) {
	shard, data := hashFilterFixture(t, 4)
	early := &testHashFilter{count: 4}
	late := &testHashFilter{count: 4}
	m := hashFilterMaterializer(t, shard, early, false)
	m.b = noHashShard{shard}
	oracle := hashFilterMaterializer(t, shard, testLabelsOnlyFilter{late}, false)
	rr := []RowRange{{From: 0, Count: 128}}
	expected, err := oracle.Materialize(context.Background(), nil, 0, data.MinTime, data.MaxTime, false, rr)
	require.NoError(t, err)
	actual, err := m.Materialize(context.Background(), nil, 0, data.MinTime, data.MaxTime, false, rr)
	require.NoError(t, err)
	require.Equal(t, collectHashFilterResults(t, expected), collectHashFilterResults(t, actual))
	require.Equal(t, 128, early.labelsCalls)
	require.Zero(t, early.hashCalls)
	require.Equal(t, 1, early.closes)
}

type hashEncodingFile struct {
	storage.ParquetFileView
	encoding string
}

func (f hashEncodingFile) Lookup(key string) (string, bool) {
	if key == schema.SeriesHashEncodingMd {
		return f.encoding, f.encoding != ""
	}
	return f.ParquetFileView.Lookup(key)
}

type hashEncodingShard struct {
	storage.ParquetShard
	encoding string
}

func (s hashEncodingShard) LabelsFile() storage.ParquetFileView {
	return hashEncodingFile{s.ParquetShard.LabelsFile(), s.encoding}
}

func TestEarlyHashFilterInvalidHashFallsBack(t *testing.T) {
	for _, value := range []string{"", "bad", "too-long-hash-value"} {
		t.Run(fmt.Sprintf("bytes=%d", len(value)), func(t *testing.T) {
			shard := buildFile(t, []struct {
				Hash string `parquet:"s_series_hash,optional"`
			}{{Hash: "12345678"}, {Hash: value}})
			m := &Materializer{b: hashEncodingShard{shard, schema.SeriesHashStableV1}, concurrency: 4, dataBytesQuota: UnlimitedQuota(), partitioner: util.NewGapBasedPartitioner(0)}
			rr := []RowRange{{From: 0, Count: 2}}
			actual, hashes, filtered, err := m.filterBySeriesHash(context.Background(), 0, rr, func(uint64) bool { return true })
			require.NoError(t, err)
			require.False(t, filtered)
			require.Nil(t, hashes)
			require.Equal(t, rr, actual)
		})
	}
}

func TestMaterializeUnknownHashEncodingFallsBack(t *testing.T) {
	shard, data := hashFilterFixture(t, 4)
	for _, encoding := range []string{"", "foreign-little-endian-v1"} {
		for _, projection := range []bool{false, true} {
			t.Run(fmt.Sprintf("encoding=%q/projection=%v", encoding, projection), func(t *testing.T) {
				f := &testHashFilter{count: 4}
				m := hashFilterMaterializer(t, shard, f, projection)
				m.b = hashEncodingShard{shard, encoding}
				oracle := hashFilterMaterializer(t, shard, &testHashFilter{count: 4}, projection)
				var hints *prom_storage.SelectHints
				if projection {
					hints = &prom_storage.SelectHints{ProjectionInclude: true, ProjectionLabels: []string{"unique", schema.SeriesHashColumn}}
				}
				rr := []RowRange{{From: 0, Count: 128}}
				expected, err := oracle.Materialize(context.Background(), hints, 0, data.MinTime, data.MaxTime, true, rr)
				require.NoError(t, err)
				actual, err := m.Materialize(context.Background(), hints, 0, data.MinTime, data.MaxTime, true, rr)
				require.NoError(t, err)
				require.Equal(t, collectHashFilterResults(t, expected), collectHashFilterResults(t, actual))
				require.Equal(t, 128, f.labelsCalls)
				require.Zero(t, f.hashCalls)
				require.Equal(t, 1, f.closes)
			})
		}
	}
}

func BenchmarkMaterializeShardFiltering(b *testing.B) {
	shard, data := hashFilterFixture(b, 24)
	for _, count := range []uint64{1, 2, 16} {
		for _, early := range []bool{false, true} {
			b.Run(fmt.Sprintf("shards=%d/early=%v", count, early), func(b *testing.B) {
				f := &testHashFilter{count: count}
				var filter MaterializedLabelsFilter = f
				if !early {
					filter = testLabelsOnlyFilter{f}
				}
				m := hashFilterMaterializer(b, shard, filter, false)
				rr := []RowRange{{From: 0, Count: 128}}
				b.ReportAllocs()
				b.ResetTimer()
				for b.Loop() {
					set, err := m.Materialize(context.Background(), nil, 0, data.MinTime, data.MaxTime, true, rr)
					if err != nil {
						b.Fatal(err)
					}
					for set.Next() {
					}
					if err := set.Err(); err != nil {
						b.Fatal(err)
					}
					if err := set.Close(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

type observedHashFile struct {
	storage.ParquetFileView
	reads *atomic.Int32
}

func (f observedHashFile) GetPages(ctx context.Context, cc parquet.ColumnChunk, minOffset, maxOffset int64) (parquet.Pages, error) {
	col, ok := f.Schema().Lookup(schema.SeriesHashColumn)
	if ok && cc.Column() == col.ColumnIndex {
		f.reads.Add(1)
	}
	return f.ParquetFileView.GetPages(ctx, cc, minOffset, maxOffset)
}

type observedHashShard struct {
	storage.ParquetShard
	reads *atomic.Int32
}

func (s observedHashShard) LabelsFile() storage.ParquetFileView {
	return observedHashFile{s.ParquetShard.LabelsFile(), s.reads}
}

func TestMaterializeReusesProjectedHash(t *testing.T) {
	shard, data := hashFilterFixture(t, 4)
	for _, hints := range []*prom_storage.SelectHints{
		nil,
		{ProjectionInclude: true, ProjectionLabels: []string{"unique", schema.SeriesHashColumn}},
		{ProjectionInclude: false, ProjectionLabels: []string{"label_name_0"}},
	} {
		var reads atomic.Int32
		f := &testHashFilter{count: 4}
		m := hashFilterMaterializer(t, observedHashShard{shard, &reads}, f, true)
		set, err := m.Materialize(context.Background(), hints, 0, data.MinTime, data.MaxTime, true, []RowRange{{From: 0, Count: 128}})
		require.NoError(t, err)
		require.NotEmpty(t, collectHashFilterResults(t, set))
		require.Equal(t, int32(1), reads.Load(), "the hash page must be read only once, including when projection requests it")
	}
}

type replacedLabelsShard struct {
	storage.ParquetShard
	file storage.ParquetFileView
}

func (s replacedLabelsShard) LabelsFile() storage.ParquetFileView { return s.file }

func withInvalidStoredHash(t testing.TB, shard storage.ParquetShard, value parquet.Value) storage.ParquetShard {
	t.Helper()
	input := shard.LabelsFile()
	group := parquet.Group{}
	for _, field := range input.Schema().Fields() {
		if field.Name() == schema.SeriesHashColumn {
			group[field.Name()] = parquet.Optional(parquet.Leaf(parquet.ByteArrayType))
		} else {
			group[field.Name()] = field
		}
	}
	sc := parquet.NewSchema(input.Schema().Name(), group)
	col, ok := sc.Lookup(schema.SeriesHashColumn)
	require.True(t, ok)
	name := filepath.Join(t.TempDir(), "labels.parquet")
	out, err := os.Create(name)
	require.NoError(t, err)
	opts := []parquet.WriterOption{sc}
	for _, kv := range input.Metadata().KeyValueMetadata {
		opts = append(opts, parquet.KeyValueMetadata(kv.Key, kv.Value))
	}
	w := parquet.NewWriter(out, opts...)
	rowID := 0
	for _, rg := range input.RowGroups() {
		rows := rg.Rows()
		buf := make([]parquet.Row, 32)
		for {
			n, err := rows.ReadRows(buf)
			require.True(t, err == nil || err == io.EOF)
			for i := range n {
				for j, v := range buf[i] {
					if v.Column() != col.ColumnIndex {
						continue
					}
					def := 1
					if rowID == 100 {
						v = value
					}
					if v.IsNull() {
						def = 0
					}
					buf[i][j] = v.Level(0, def, col.ColumnIndex)
				}
				rowID++
			}
			_, writeErr := w.WriteRows(buf[:n])
			require.NoError(t, writeErr)
			if err == io.EOF {
				break
			}
		}
		require.NoError(t, rows.Close())
	}
	require.NoError(t, w.Close())
	require.NoError(t, out.Close())
	f, err := storage.OpenFromFile(context.Background(), name)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, f.Close()) })
	return replacedLabelsShard{shard, f}
}

func TestMaterializeInvalidHashFallsBackToLabels(t *testing.T) {
	shard, data := hashFilterFixture(t, 4)
	for _, tt := range []struct {
		name  string
		value parquet.Value
	}{
		{"null", parquet.NullValue()},
		{"short", parquet.ValueOf([]byte("bad"))},
		{"long", parquet.ValueOf([]byte("too-long-hash"))},
	} {
		t.Run(tt.name, func(t *testing.T) {
			invalid := withInvalidStoredHash(t, shard, tt.value)
			for _, projection := range []bool{false, true} {
				f := &testHashFilter{count: 4}
				actual := hashFilterMaterializer(t, invalid, f, projection)
				oracle := hashFilterMaterializer(t, shard, &testHashFilter{count: 4}, projection)
				var hints *prom_storage.SelectHints
				if projection {
					hints = &prom_storage.SelectHints{ProjectionInclude: true, ProjectionLabels: []string{"unique", schema.SeriesHashColumn}}
				}
				rr := []RowRange{{From: 0, Count: 128}}
				expected, err := oracle.Materialize(context.Background(), hints, 0, data.MinTime, data.MaxTime, false, rr)
				require.NoError(t, err)
				result, err := actual.Materialize(context.Background(), hints, 0, data.MinTime, data.MaxTime, false, rr)
				require.NoError(t, err)
				require.Equal(t, collectHashFilterResults(t, expected), collectHashFilterResults(t, result))
				require.Equal(t, 128, f.labelsCalls)
				require.Equal(t, 1, f.closes)
			}
		})
	}
}
