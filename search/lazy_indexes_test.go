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
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
	"github.com/prometheus-community/parquet-common/util"
)

type contextCheckingBucket struct{ objstore.Bucket }

func (b contextCheckingBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.Bucket.GetRange(ctx, name, off, length)
}

func TestLazyPageIndexes(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { require.NoError(t, st.Close()) })
	data := util.GenerateTestData(t, st, ctx, util.TestConfig{TotalMetricNames: 1, MetricsPerMetricName: 64, NumberOfLabels: 16, RandomLabels: 1, NumberOfSamples: 8})
	inner, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	bkt := contextCheckingBucket{inner}
	t.Cleanup(func() { require.NoError(t, bkt.Close()) })
	eager := convertToParquet(t, ctx, bkt, data, st.Head())
	t.Cleanup(func() { require.NoError(t, eager.(interface{ Close() error }).Close()) })
	expected := query(t, data.MinTime, data.MaxTime, eager)
	require.Len(t, expected, 64)
	open := func() *storage.ParquetShardOpener {
		openCtx, cancel := context.WithCancel(ctx)
		opener := storage.NewParquetBucketOpener(bkt)
		shard, err := storage.NewParquetShardOpener(openCtx, "shard", opener, opener, 0, storage.WithFileOptions(parquet.SkipMagicBytes(true), parquet.SkipBloomFilters(true), parquet.SkipPageIndex(true)))
		cancel()
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, shard.Close()) })
		require.Empty(t, shard.LabelsFile().ColumnIndexes())
		require.Empty(t, shard.ChunksFile().ColumnIndexes())
		return shard
	}
	lazy := open()
	// Concurrent queries must safely populate the same per-column indexes after
	// the original opening request has ended.
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			actual := query(t, data.MinTime, data.MaxTime, lazy)
			require.Len(t, actual, len(expected))
			for i := range actual {
				require.True(t, labels.Equal(expected[i].Labels(), actual[i].Labels()))
				want := expected[i].Iterator(nil)
				got := actual[i].Iterator(nil)
				for want.Next() {
					require.True(t, got.Next())
					require.Equal(t, want.At().Chunk.Bytes(), got.At().Chunk.Bytes())
				}
				require.False(t, got.Next())
				require.NoError(t, want.Err())
				require.NoError(t, got.Err())
			}
		})
	}
	wg.Wait()
	eq := Equal(schema.LabelToColumn("unique"), parquet.ValueOf("unique_5"))
	require.Len(t, query(t, data.MinTime, data.MaxTime, open(), eq), 1)
	matcher := labels.MustNewMatcher(labels.MatchRegexp, "unique", "unique_[123].*")
	rc, err := Regex(schema.LabelToColumn("unique"), matcher)
	require.NoError(t, err)
	require.NotEmpty(t, query(t, data.MinTime, data.MaxTime, open(), rc))
	// Cancellation of a new query still applies to lazy index reads.
	cold := open()
	s, err := cold.TSDBSchema()
	require.NoError(t, err)
	m, err := NewMaterializer(s, schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool()), cold, 4, UnlimitedQuota(), UnlimitedQuota(), UnlimitedQuota(), NoopMaterializedSeriesFunc, NoopMaterializedLabelsFilterCallback, false)
	require.NoError(t, err)
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	_, err = m.Materialize(cancelled, nil, 0, data.MinTime, data.MaxTime, true, []RowRange{{From: 0, Count: 64}})
	require.ErrorIs(t, err, context.Canceled)
}

func BenchmarkOpenWidePageIndexes(b *testing.B) {
	group := parquet.Group{}
	for col := range 128 {
		group[fmt.Sprintf("label_%03d", col)] = parquet.Optional(parquet.String())
	}
	sc := parquet.NewSchema("wide", group)
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, sc, parquet.PageBufferSize(1024))
	for row := range 512 {
		values := make(parquet.Row, 128)
		for col := range values {
			values[col] = parquet.ValueOf(fmt.Sprintf("%06d-%06d", row, col)).Level(0, 1, col)
		}
		_, err := w.WriteRows([]parquet.Row{values})
		require.NoError(b, err)
	}
	require.NoError(b, w.Close())
	data := buf.Bytes()
	for _, lazy := range []bool{false, true} {
		b.Run(fmt.Sprintf("lazy=%v", lazy), func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)), parquet.SkipPageIndex(lazy), parquet.SkipBloomFilters(true))
				if err != nil {
					b.Fatal(err)
				}
				if len(f.RowGroups()) != 1 {
					b.Fatal("row group missing")
				}
			}
		})
	}
}
