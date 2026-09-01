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

package convert

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	"github.com/prometheus/prometheus/util/teststorage"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore/providers/filesystem"

	"github.com/prometheus-community/parquet-common/schema"
	"github.com/prometheus-community/parquet-common/storage"
)

func Test_Convert_TSDB(t *testing.T) {
	ctx := context.Background()

	tc := []struct {
		dataColDuration        time.Duration
		step                   time.Duration
		numberOfSamples        int
		expectedNumberOfChunks int
		expectedPointsPerChunk int
	}{
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        3,
			expectedNumberOfChunks: 3,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        48,
			expectedNumberOfChunks: 48,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        10,
			expectedNumberOfChunks: 1,
			expectedPointsPerChunk: 10,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        32,
			expectedNumberOfChunks: 2,
			expectedPointsPerChunk: 16,
		},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("dataColDurationMs:%v,step:%v,numberOfSamples:%v", tt.dataColDuration.Hours(), tt.step.Seconds(), tt.numberOfSamples), func(t *testing.T) {
			st := teststorage.New(t)
			t.Cleanup(func() { _ = st.Close() })

			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = bkt.Close() })

			app := st.Appender(ctx)
			seriesHash := make(map[uint64]struct{})
			totalSeries := 1200
			for i := 0; i != totalSeries; i++ {
				for j := 0; j < tt.numberOfSamples; j++ {
					lbls := labels.FromStrings("__name__", "foo", "bar", fmt.Sprintf("%d", 2*i))
					seriesHash[lbls.Hash()] = struct{}{}
					_, err := app.Append(0, lbls, (tt.step * time.Duration(j)).Milliseconds(), float64(i))
					require.NoError(t, err)
				}
			}

			require.NoError(t, app.Commit())
			h := st.Head()

			opts := []ConvertOption{
				WithSortBy(labels.MetricName, "bar"),
				WithColDuration(tt.dataColDuration),
				WithNumRowGroups(4),
				WithRowGroupSize(100),
				WithReadConcurrency(4),
				WithWriteConcurrency(2),
			}
			// build equivalent config for ease of assertion with methods that take cfg
			cfg := DefaultConvertOpts
			for _, opt := range opts {
				opt(&cfg)
			}

			shardCount, err := ConvertTSDBBlock(
				ctx, bkt,
				h.MinTime(), h.MaxTime(), []Convertible{h},
				promslog.NewNopLogger(),
				opts...,
			)
			require.NoError(t, err)
			require.Equal(t, 3, shardCount) // 1200 series; 3 shards of 4 row groups of 100 rows

			bucketOpener := storage.NewParquetBucketOpener(bkt)

			remainingRows := totalSeries
			allSeries := make([]labels.Labels, 0, totalSeries)
			for shardIdx := range shardCount {
				shard, err := storage.NewParquetShardOpener(
					ctx, DefaultConvertOpts.name, bucketOpener, bucketOpener, shardIdx,
				)
				require.NoError(t, err)

				require.Equal(t, len(shard.LabelsFile().RowGroups()), len(shard.ChunksFile().RowGroups()))
				series, chunks, err := readSeries(t, shard)
				require.NoError(t, err)

				if shardIdx < shardCount-1 {
					require.Equal(t, cfg.numRowGroups*cfg.rowGroupSize, len(series))
					require.Equal(t, cfg.numRowGroups*cfg.rowGroupSize, len(chunks))
				} else {
					require.Equal(t, remainingRows, len(series))
					require.Equal(t, remainingRows, len(chunks))
				}

				// Verify series hash column exists and is accessible in working context
				seriesHashIdx, ok := shard.LabelsFile().Schema().Lookup(schema.SeriesHashColumn)
				require.True(t, ok, "series hash column should exist")

				// Make sure the chunk page bounds are empty
				for _, ci := range shard.ChunksFile().ColumnIndexes() {
					for _, value := range append(ci.MinValues, ci.MaxValues...) {
						require.Empty(t, value)
					}
				}

				colIdx, ok := shard.LabelsFile().Schema().Lookup(schema.ColIndexesColumn)
				require.True(t, ok)
				seriesHashIdx, ok = shard.LabelsFile().Schema().Lookup(schema.SeriesHashColumn)
				require.True(t, ok)
				// Make sure labels pages bounds are populated with index data
				for i, ci := range shard.LabelsFile().ColumnIndexes() {
					for _, value := range append(ci.MinValues, ci.MaxValues...) {
						if i%cfg.numRowGroups == colIdx.ColumnIndex || i%cfg.numRowGroups == seriesHashIdx.ColumnIndex {
							require.Empty(t, value)
						} else {
							require.NotEmpty(t, value)
						}
					}
				}

				for i, s := range series {
					require.Contains(t, seriesHash, s.Hash())
					require.Len(t, chunks[i], tt.expectedNumberOfChunks)
					totalSamples := 0
					for _, c := range chunks[i] {
						require.Equal(t, tt.expectedPointsPerChunk, c.Chunk.NumSamples())
						totalSamples += c.Chunk.NumSamples()
					}
					require.Equal(t, tt.numberOfSamples, totalSamples)
				}
				allSeries = append(allSeries, series...)
				remainingRows -= len(series)
			}

			// make sure the series are sorted
			for i := 0; i < len(allSeries)-1; i++ {
				require.LessOrEqual(t, allSeries[i].Get(labels.MetricName), allSeries[i+1].Get(labels.MetricName))
				if allSeries[i].Get(labels.MetricName) == allSeries[i+1].Get(labels.MetricName) {
					require.LessOrEqual(t, allSeries[i].Get("bar"), allSeries[i+1].Get("bar"))
				}
			}
		})
	}
}

func Test_Convert_TSDB_SingleRowReader(t *testing.T) {
	ctx := context.Background()

	tc := []struct {
		dataColDuration        time.Duration
		step                   time.Duration
		numberOfSamples        int
		expectedNumberOfChunks int
		expectedPointsPerChunk int
	}{
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        3,
			expectedNumberOfChunks: 3,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        time.Hour,
			step:                   time.Hour,
			numberOfSamples:        48,
			expectedNumberOfChunks: 48,
			expectedPointsPerChunk: 1,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        10,
			expectedNumberOfChunks: 1,
			expectedPointsPerChunk: 10,
		},
		{
			dataColDuration:        8 * time.Hour,
			step:                   time.Hour / 2,
			numberOfSamples:        32,
			expectedNumberOfChunks: 2,
			expectedPointsPerChunk: 16,
		},
	}

	for _, tt := range tc {
		t.Run(fmt.Sprintf("dataColDurationMs:%v,step:%v,numberOfSamples:%v", tt.dataColDuration.Hours(), tt.step.Seconds(), tt.numberOfSamples), func(t *testing.T) {
			st := teststorage.New(t)
			t.Cleanup(func() { _ = st.Close() })

			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = bkt.Close() })

			app := st.Appender(ctx)
			seriesHash := make(map[uint64]struct{})
			totalSeries := 1200
			for i := 0; i != totalSeries; i++ {
				for j := 0; j < tt.numberOfSamples; j++ {
					lbls := labels.FromStrings("__name__", "foo", "bar", fmt.Sprintf("%d", 2*i))
					seriesHash[lbls.Hash()] = struct{}{}
					_, err := app.Append(0, lbls, (tt.step * time.Duration(j)).Milliseconds(), float64(i))
					require.NoError(t, err)
				}
			}

			require.NoError(t, app.Commit())
			h := st.Head()

			// Don't specify WithNumRowGroups to trigger singleTSDBRowReader path (defaults to math.MaxInt32)
			opts := []ConvertOption{
				WithSortBy(labels.MetricName, "bar"),
				WithColDuration(tt.dataColDuration),
				WithRowGroupSize(100),
				WithReadConcurrency(4),
				WithWriteConcurrency(2),
			}
			// build equivalent config for ease of assertion with methods that take cfg
			cfg := DefaultConvertOpts
			for _, opt := range opts {
				opt(&cfg)
			}

			shardCount, err := ConvertTSDBBlock(
				ctx, bkt,
				h.MinTime(), h.MaxTime(), []Convertible{h},
				promslog.NewNopLogger(),
				opts...,
			)
			require.NoError(t, err)
			// singleTSDBRowReader should produce exactly 1 shard
			require.Equal(t, 1, shardCount)

			bucketOpener := storage.NewParquetBucketOpener(bkt)

			allSeries := make([]labels.Labels, 0, totalSeries)
			for shardIdx := range shardCount {
				shard, err := storage.NewParquetShardOpener(
					ctx, DefaultConvertOpts.name, bucketOpener, bucketOpener, shardIdx,
				)
				require.NoError(t, err)

				require.Equal(t, len(shard.LabelsFile().RowGroups()), len(shard.ChunksFile().RowGroups()))
				series, chunks, err := readSeries(t, shard)
				require.NoError(t, err)

				// All series should be in the single shard
				require.Equal(t, totalSeries, len(series))
				require.Equal(t, totalSeries, len(chunks))

				// Verify series hash column exists and is accessible in working context
				_, ok := shard.LabelsFile().Schema().Lookup(schema.SeriesHashColumn)
				require.True(t, ok, "series hash column should exist")

				// Make sure the chunk page bounds are empty
				for _, ci := range shard.ChunksFile().ColumnIndexes() {
					for _, value := range append(ci.MinValues, ci.MaxValues...) {
						require.Empty(t, value)
					}
				}

				// For singleTSDBRowReader, verify the basic structure is correct
				// The column index structure may differ from the sharded case,
				// so we focus on verifying all data is present rather than detailed index structure
				_, ok = shard.LabelsFile().Schema().Lookup(schema.ColIndexesColumn)
				require.True(t, ok)
				// Verify that column indexes exist (structure may differ from sharded case)
				columnIndexes := shard.LabelsFile().ColumnIndexes()
				require.NotEmpty(t, columnIndexes, "column indexes should exist")

				for i, s := range series {
					require.Contains(t, seriesHash, s.Hash())
					require.Len(t, chunks[i], tt.expectedNumberOfChunks)
					totalSamples := 0
					for _, c := range chunks[i] {
						require.Equal(t, tt.expectedPointsPerChunk, c.Chunk.NumSamples())
						totalSamples += c.Chunk.NumSamples()
					}
					require.Equal(t, tt.numberOfSamples, totalSamples)
				}
				allSeries = append(allSeries, series...)
			}

			// make sure the series are sorted
			for i := 0; i < len(allSeries)-1; i++ {
				require.LessOrEqual(t, allSeries[i].Get(labels.MetricName), allSeries[i+1].Get(labels.MetricName))
				if allSeries[i].Get(labels.MetricName) == allSeries[i+1].Get(labels.MetricName) {
					require.LessOrEqual(t, allSeries[i].Get("bar"), allSeries[i+1].Get("bar"))
				}
			}
		})
	}
}

func BenchmarkConvertTSDBParallel(b *testing.B) {
	ctx := context.Background()

	testCases := []struct {
		shards                int
		shardWriteParallelism int
	}{
		// first set of cases convert shards serially (write parallelism = 1)
		{
			shards:                1,
			shardWriteParallelism: 1,
		},
		{
			shards:                2,
			shardWriteParallelism: 1,
		},
		{
			shards:                4,
			shardWriteParallelism: 1,
		},
		{
			shards:                8,
			shardWriteParallelism: 1,
		},
		// remaining parallel cases seek to increase parallelism to reduce conversion time
		// and demonstrate diminishing returns as CPU cores become saturated.
		{
			shards:                2,
			shardWriteParallelism: 2,
		},
		{
			shards:                4,
			shardWriteParallelism: 2,
		},
		{
			shards:                4,
			shardWriteParallelism: 4,
		},
		{
			shards:                8,
			shardWriteParallelism: 2,
		},
		{
			shards:                8,
			shardWriteParallelism: 4,
		},
		{
			shards:                8,
			shardWriteParallelism: 8,
		},
	}

	st := teststorage.New(b)
	b.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(b.TempDir())
	require.NoError(b, err)
	b.Cleanup(func() { _ = bkt.Close() })

	// 100,000 series
	metrics := 50
	instances := 2
	regions := 4
	zones := 2
	services := 25
	environments := 5

	numSeries := metrics * instances * regions * zones * services * environments
	b.Logf("Generating %d series (%d metrics × %d instances × %d regions × %d zones × %d services × %d environments)",
		numSeries, metrics, instances, regions, zones, services, environments)

	step := 15 * time.Second
	blockDuration := 2 * time.Hour
	numSamples := int(blockDuration / step)

	app := st.Appender(ctx)
	for metric := range metrics {
		for instance := range instances {
			for region := range regions {
				for zone := range zones {
					for service := range services {
						for env := range environments {
							lbls := labels.FromStrings(
								"__name__", fmt.Sprintf("test_metric_%d", metric),
								"instance", fmt.Sprintf("instance-%d", instance),
								"region", fmt.Sprintf("region-%d", region),
								"zone", fmt.Sprintf("zone-%d", zone),
								"service", fmt.Sprintf("service-%d", service),
								"environment", fmt.Sprintf("environment-%d", env),
							)
							for sample := range numSamples {
								_, err := app.Append(0, lbls, (step * time.Duration(sample)).Milliseconds(), float64(instance))
								require.NoError(b, err)
							}
						}
					}
				}
			}
		}
	}
	require.NoError(b, app.Commit())

	for _, tc := range testCases {
		for _, readParallelismPerWrite := range []int{8, 16, 24, 32} {
			tcName := fmt.Sprintf(
				"shards=%d/writeParallelism=%d/readParallelismPerWrite=%d",
				tc.shards, tc.shardWriteParallelism, readParallelismPerWrite,
			)
			b.Run(tcName, func(b *testing.B) {
				h := st.Head()
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					// Input block size is constant;
					// to easily target the varying shard counts for the test cases.
					// hold max row groups constant and calculate row group sizes.
					const numRowGroups = 4
					opts := []ConvertOption{
						WithSortBy(labels.MetricName),
						WithNumRowGroups(numRowGroups),
						WithWriteConcurrency(tc.shardWriteParallelism),
						// Read concurrency is per-shardWriter;
						// With a 16-core or 32-core dev machine and test cases up to 8 shards written in parallel
						// 4 readers per writer allows benchmark to show diminishing returns as CPU cores become saturated.
						// A 4:1 ratio of reader to writer concurrency minimizes total conversion time in this benchmark.
						WithReadConcurrency(readParallelismPerWrite),
					}

					// Calculate row group size limit to target shard count for the test case.
					shardRows := (numSeries + tc.shards - 1) / tc.shards
					rowGroupSize := (shardRows + numRowGroups - 1) / numRowGroups
					opts = append(opts, WithRowGroupSize(rowGroupSize))

					var outputShards int
					var err error

					start := time.Now()
					outputShards, err = ConvertTSDBBlock(
						ctx, bkt,
						h.MinTime(), h.MaxTime(), []Convertible{h},
						promslog.NewNopLogger(),
						opts...,
					)

					require.NoError(b, err)
					dur := time.Since(start)
					b.ReportMetric(float64(dur.Nanoseconds()/int64(b.N)), "conversion-ns/op")
					require.Equal(b, tc.shards, outputShards)
				}
			})
		}
	}
}

func Test_CreateParquetWithReducedTimestampSamples(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)

	// 240 samples * 30 seconds = 2 hours
	step := (30 * time.Second).Milliseconds()
	for i := 0; i < 240; i++ {
		_, err := app.Append(0, labels.FromStrings("__name__", "test", "foo", "bar"), int64(i)*step, float64(i))
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())

	h := st.Head()
	mint, maxt := (time.Minute * 30).Milliseconds(), (time.Minute*90).Milliseconds()-1

	datColDuration := time.Minute * 10
	shards, err := ConvertTSDBBlock(
		ctx, bkt, mint, maxt,
		[]Convertible{h},
		promslog.NewNopLogger(),
		WithColDuration(datColDuration),
		WithSortBy(labels.MetricName),
		WithColumnPageBuffers(parquet.NewFileBufferPool(t.TempDir(), "buffers.*")),
	)

	require.NoError(t, err)
	require.Equal(t, 1, shards)

	bucketOpener := storage.NewParquetBucketOpener(bkt)
	shard, err := storage.NewParquetShardOpener(
		ctx, DefaultConvertOpts.name, bucketOpener, bucketOpener, 0,
	)
	require.NoError(t, err)

	// Check metadatas
	for _, file := range []storage.ParquetFileView{shard.LabelsFile(), shard.ChunksFile()} {
		require.Equal(t, schema.MetadataToMap(file.Metadata().KeyValueMetadata)[schema.MinTMd], strconv.FormatInt(mint, 10))
		require.Equal(t, schema.MetadataToMap(file.Metadata().KeyValueMetadata)[schema.MaxTMd], strconv.FormatInt(maxt, 10))
		require.Equal(t, schema.MetadataToMap(file.Metadata().KeyValueMetadata)[schema.DataColSizeMd], strconv.FormatInt(datColDuration.Milliseconds(), 10))
	}

	// 2 labels + col indexes + series hash
	require.Len(t, shard.LabelsFile().Schema().Columns(), 4)
	// 6 data cols with 10 min duration
	require.Len(t, shard.ChunksFile().Schema().Columns(), 6)
	series, chunks, err := readSeries(t, shard)

	require.NoError(t, err)
	require.Len(t, series, 1)
	require.Len(t, chunks, 1)
	require.Equal(t, labels.FromStrings("__name__", "test", "foo", "bar").Hash(), series[0].Hash())

	totalSamples := 0
	for _, c := range chunks[0] {
		totalSamples += c.Chunk.NumSamples()
		require.LessOrEqual(t, c.MaxTime, maxt)
		require.GreaterOrEqual(t, c.MinTime, mint)
	}
	require.Equal(t, 120, totalSamples)
}

func Test_BlockHasOnlySomeSeriesInConvertTime(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	app := st.Appender(ctx)

	// one series before convert time
	_, err = app.Append(0, labels.FromStrings(
		labels.MetricName, fmt.Sprintf("metric_%d", 0),
		"i", fmt.Sprintf("%v", 0),
	), 0, float64(0))
	require.NoError(t, err)

	// one series inside convert time
	_, err = app.Append(0, labels.FromStrings(
		labels.MetricName, fmt.Sprintf("metric_%d", 1),
		"i", fmt.Sprintf("%v", 1),
	), 11, float64(0))
	require.NoError(t, err)

	// one series after convert time
	_, err = app.Append(0, labels.FromStrings(
		labels.MetricName, fmt.Sprintf("metric_%d", 2),
		"i", fmt.Sprintf("%v", 2),
	), 21, float64(0))
	require.NoError(t, err)

	// many series inside convert time
	for i := 0; i != 240; i++ {
		_, err = app.Append(0, labels.FromStrings(
			labels.MetricName, fmt.Sprintf("metric_%d", i+3),
			"i", fmt.Sprintf("%v", 1),
		), 11, float64(0))
		require.NoError(t, err)
	}

	require.NoError(t, app.Commit())

	h := st.Head()

	shards, err := ConvertTSDBBlock(
		ctx,
		bkt,
		10,
		20-1,
		[]Convertible{h},
		promslog.NewNopLogger(),
		WithColDuration(time.Millisecond*10),
		WithColumnPageBuffers(parquet.NewFileBufferPool(t.TempDir(), "buffers.*")),
	)
	require.NoError(t, err)
	require.Equal(t, 1, shards)

	bucketOpener := storage.NewParquetBucketOpener(bkt)
	shard, err := storage.NewParquetShardOpener(
		ctx, DefaultConvertOpts.name, bucketOpener, bucketOpener, 0,
	)
	require.NoError(t, err)

	series, _, err := readSeries(t, shard)
	require.NoError(t, err)
	require.Len(t, series, 241)
}

func Test_SortedLabels(t *testing.T) {
	ctx := context.Background()

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	numberOfBLocks := 10
	totalSeries := 0
	storages := make([]*teststorage.TestStorage, numberOfBLocks)
	heads := make([]Convertible, numberOfBLocks)
	for i := 0; i < numberOfBLocks; i++ {
		storages[i] = teststorage.New(t)
		t.Cleanup(func() { _ = storages[i].Close() })
		heads[i] = storages[i].Head()
	}

	for si, s := range storages {
		app := s.Appender(ctx)
		// Some very random series
		for i := 0; i < 240; i++ {
			_, err := app.Append(0, labels.FromStrings(
				labels.MetricName, fmt.Sprintf("%v", rand.Int31()),
				"type", fmt.Sprintf("block_%v", si),
				"zzz", fmt.Sprintf("%v", rand.Int31()),
				"i", fmt.Sprintf("%v", i),
			), 10, float64(i))
			require.NoError(t, err)
			totalSeries++
		}
		require.NoError(t, app.Commit())
	}

	// Lets create some common series on both blocks
	name := "duplicated"
	zzz := "duplicated"
	for i := 0; i < 240; i++ {
		for j := 0; j < 2; j++ {
			app := storages[int(rand.Int31())%len(storages)].Appender(ctx)
			lbls := labels.FromStrings(
				labels.MetricName, name,
				"type", "duplicated",
				"zzz", zzz,
				"i", fmt.Sprintf("%v", i),
			)

			_, err := app.Append(0, lbls, int64(j), float64(i))
			require.NoError(t, err)
			require.NoError(t, app.Commit())
		}
		totalSeries++
	}

	require.Equal(t, totalSeries, 2640)
	numRowGroups := 1
	rowGroupSize := 1000
	expectedShardCount := (totalSeries + (numRowGroups * rowGroupSize) - 1) / (numRowGroups * rowGroupSize)
	expectedRowsPerShard := (totalSeries + expectedShardCount - 1) / expectedShardCount

	// lets sort first by `zzz` as its not the default sorting on TSDB
	shardCount, err := ConvertTSDBBlock(
		ctx,
		bkt,
		0,
		time.Minute.Milliseconds(),
		heads,
		promslog.NewNopLogger(),
		WithColDuration(time.Minute*10),
		WithSortBy("zzz", labels.MetricName),
		WithColumnPageBuffers(parquet.NewFileBufferPool(t.TempDir(), "buffers.*")),
		WithNumRowGroups(numRowGroups),
		WithRowGroupSize(rowGroupSize),
	)
	require.NoError(t, err)
	require.Equal(t, expectedShardCount, shardCount)

	remainingRows := totalSeries
	for i := range shardCount {
		bucketOpener := storage.NewParquetBucketOpener(bkt)
		shard, err := storage.NewParquetShardOpener(
			ctx, DefaultConvertOpts.name, bucketOpener, bucketOpener, i,
		)
		require.NoError(t, err)

		series, chunks, err := readSeries(t, shard)
		require.NoError(t, err)
		if i < shardCount-1 {
			require.Equal(t, expectedRowsPerShard, len(series))
			require.Equal(t, expectedRowsPerShard, len(chunks))
		} else {
			require.Equal(t, remainingRows, len(series))
			require.Equal(t, remainingRows, len(chunks))
		}
		remainingRows -= len(series)

		for i := 0; i < len(series)-1; i++ {
			require.LessOrEqual(t, series[i].Get("zzz"), series[i+1].Get("zzz"))
			if series[i].Get("zzz") == series[i+1].Get("zzz") {
				require.LessOrEqual(t, series[i].Get(labels.MetricName), series[i+1].Get(labels.MetricName))
			}
			require.Len(t, chunks[i], 1)
			st := chunks[i][0].Chunk.Iterator(nil)
			expectedSamples := 1
			if series[i].Get("type") == "duplicated" {
				expectedSamples++
			}
			totalSamples := 0

			for st.Next() != chunkenc.ValNone {
				totalSamples++
			}

			require.Equal(t, expectedSamples, totalSamples, "series", series[i])

			require.NoError(t, st.Err())
		}
	}
	require.Equal(t, 0, remainingRows)
}

// Test_TooManyColumnsPanic reproduces a panic that occurs when the Parquet file
// would exceed the maximum number of columns supported by parquet-go.
// This catches a bug where the conversion did not correctly release resources on errors
// during schema building, leading to a deadlock. If the column limit issue is fixed,
// this test should be updated to still trigger an error during schema building that leads to
// resource cleanup.
func Test_TooManyColumnsPanic(t *testing.T) {
	ctx := context.Background()
	st := teststorage.New(t)
	t.Cleanup(func() { _ = st.Close() })

	bkt, err := filesystem.NewBucket(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bkt.Close() })

	// Just a single series, but with too many columns
	const tooManyColumns = parquet.MaxColumnIndex + 2
	app := st.Appender(ctx)
	lbls := make([]string, tooManyColumns*2)
	for i := range tooManyColumns {
		lbls[2*i] = fmt.Sprintf("key_%d ", i)
		lbls[2*i+1] = "value"
	}
	_, err = app.Append(0, labels.FromStrings(lbls...), 1, 2.0)
	require.NoError(t, err)
	require.NoError(t, app.Commit())

	// This is important and is what reproduces the deadlock: we don't convert
	// the head, we convert a compacted on-disk block. That way, if the underlying
	// block is opened and not closed properly, a deadlock occurs when closing the
	// storage.
	err = st.CompactHead(tsdb.NewRangeHead(st.Head(), 0, 2))
	require.NoError(t, err)
	require.Len(t, st.Blocks(), 1)

	require.Panics(t, func() {
		_, err = ConvertTSDBBlock(
			ctx, bkt, 0, 2,
			[]Convertible{st.Blocks()[0]},
			promslog.NewNopLogger(),
		)
	})
}

// createHighCardinalityTestData creates TSDB test data with high cardinality label names.
// Each unique label name becomes a column in the Parquet file, plus system columns.
// Returns the storage, head block pointer, and the number of unique label names created.
// The caller is responsible for cleaning up the storage.
func createHighCardinalityTestData(t *testing.T, uniqueLabelNames, labelsPerSeries int) (*teststorage.TestStorage, *tsdb.Head, int) {
	ctx := context.Background()
	st := teststorage.New(t)

	app := st.Appender(ctx)
	labelNameCounter := 0

	// Create series with high cardinality label names
	// Each series gets unique label names, ensuring we have many unique label names total
	// We'll create enough series to exceed the column limit
	numSeries := (uniqueLabelNames + labelsPerSeries - 1) / labelsPerSeries
	for seriesIdx := 0; seriesIdx < numSeries && labelNameCounter < uniqueLabelNames; seriesIdx++ {
		// Create labels for this series
		seriesLabels := make([]string, 0, labelsPerSeries*2+2)
		seriesLabels = append(seriesLabels, labels.MetricName, fmt.Sprintf("metric_%d", seriesIdx))

		// Add unique label names to this series
		for i := 0; i < labelsPerSeries && labelNameCounter < uniqueLabelNames; i++ {
			labelName := fmt.Sprintf("high_cardinality_label_%d", labelNameCounter)
			seriesLabels = append(seriesLabels, labelName, fmt.Sprintf("value_%d", seriesIdx))
			labelNameCounter++
		}

		// Append a sample for this series
		_, err := app.Append(0, labels.FromStrings(seriesLabels...), int64(seriesIdx), float64(seriesIdx))
		require.NoError(t, err)

		// Commit periodically to avoid memory issues
		if seriesIdx%1000 == 0 && seriesIdx > 0 {
			require.NoError(t, app.Commit())
			app = st.Appender(ctx)
		}
	}

	require.NoError(t, app.Commit())
	require.GreaterOrEqual(t, labelNameCounter, uniqueLabelNames, "Should have created enough unique label names")
	t.Logf("Created %d unique label names (target: %d)", labelNameCounter, uniqueLabelNames)

	h := st.Head()

	// Verify we actually have the unique label names in the TSDB block
	indexr, err := h.Index()
	require.NoError(t, err)
	defer func() {
		_ = indexr.Close()
	}()
	labelNames, err := indexr.LabelNames(ctx)
	require.NoError(t, err)
	// __name__ is always present, so we expect at least labelNameCounter + 1 unique label names
	t.Logf("TSDB block has %d unique label names", len(labelNames))
	require.GreaterOrEqual(t, len(labelNames), uniqueLabelNames, "TSDB block should have at least the expected number of unique label names")

	return st, h, labelNameCounter
}

// verifyShardsWithLimit verifies that all shards are valid and don't exceed the specified column limit.
func verifyShardsWithLimit(t *testing.T, ctx context.Context, bkt *filesystem.Bucket, shardCount int, maxNumColumns int) {
	bucketOpener := storage.NewParquetBucketOpener(bkt)

	for shardIdx := range shardCount {
		shard, err := storage.NewParquetShardOpener(
			ctx, DefaultConvertOpts.name, bucketOpener, bucketOpener, shardIdx,
		)
		require.NoError(t, err)

		labelColumns := shard.LabelsFile().Schema().Columns()
		totalColumns := len(labelColumns)

		// Verify the shard doesn't exceed the column limit
		require.LessOrEqual(t, totalColumns, maxNumColumns,
			"Shard %d should not exceed Parquet column limit of %d", shardIdx, maxNumColumns)

		// Verify we can read from the shard
		series, chunks, err := readSeries(t, shard)
		require.NoError(t, err)
		require.NotEmpty(t, series, "Shard %d should contain series", shardIdx)
		require.Equal(t, len(series), len(chunks), "Shard %d should have matching series and chunks", shardIdx)
	}
}

// Test_TooManyColumns verifies that conversion correctly shards based on column limits
// both when numRowGroups is set and when it's not.
func Test_TooManyColumns(t *testing.T) {
	ctx := context.Background()

	tc := []struct {
		name             string
		withNumRowGroups bool
		description      string
		maxNumColumns    int // Maximum columns per shard (includes 2 system columns)
		uniqueLabelNames int // Total unique label names to create
		labelsPerSeries  int // Number of unique labels per series
		minShards        int // Minimum expected number of shards
	}{
		{
			name:             "with_numRowGroups_2_shards",
			withNumRowGroups: true,
			description:      "Uses shardedTSDBRowReaders path when numRowGroups is set, creates 2 shards",
			maxNumColumns:    20, // 18 label columns + 2 system columns
			uniqueLabelNames: 30, // Exceeds maxNumColumns to trigger sharding
			labelsPerSeries:  10, // Each series will have 10 unique labels (plus __name__)
			minShards:        2,
		},
		{
			name:             "without_numRowGroups_2_shards",
			withNumRowGroups: false,
			description:      "Uses singleTSDBRowReader path when numRowGroups is not set, creates 2 shards",
			maxNumColumns:    20, // 18 label columns + 2 system columns
			uniqueLabelNames: 30, // Exceeds maxNumColumns to trigger sharding
			labelsPerSeries:  10, // Each series will have 10 unique labels (plus __name__)
			minShards:        2,
		},
		{
			name:             "with_numRowGroups_3_shards",
			withNumRowGroups: true,
			description:      "Uses shardedTSDBRowReaders path when numRowGroups is set, creates 3+ shards",
			maxNumColumns:    20, // 18 label columns + 2 system columns
			uniqueLabelNames: 50, // Will require at least 3 shards (50 / 18 ≈ 2.78)
			labelsPerSeries:  15, // Each series will have 15 unique labels (plus __name__)
			minShards:        3,
		},
		{
			name:             "without_numRowGroups_3_shards",
			withNumRowGroups: false,
			description:      "Uses singleTSDBRowReader path when numRowGroups is not set, creates 3+ shards",
			maxNumColumns:    20, // 18 label columns + 2 system columns
			uniqueLabelNames: 50, // Will require at least 3 shards (50 / 18 ≈ 2.78)
			labelsPerSeries:  15, // Each series will have 15 unique labels (plus __name__)
			minShards:        3,
		},
	}

	for _, tt := range tc {
		t.Run(tt.name, func(t *testing.T) {
			bkt, err := filesystem.NewBucket(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { _ = bkt.Close() })

			st, h, _ := createHighCardinalityTestData(t, tt.uniqueLabelNames, tt.labelsPerSeries)
			t.Cleanup(func() { _ = st.Close() })

			opts := []ConvertOption{
				WithMaxNumColumns(tt.maxNumColumns),
			}
			if tt.withNumRowGroups {
				opts = append(opts, WithNumRowGroups(4), WithRowGroupSize(1000))
			}

			shardCount, err := ConvertTSDBBlock(
				ctx, bkt,
				h.MinTime(), h.MaxTime(),
				[]Convertible{h},
				promslog.NewNopLogger(),
				opts...,
			)
			require.NoError(t, err, "Conversion should succeed by creating multiple shards: %s", tt.description)
			require.GreaterOrEqual(t, shardCount, tt.minShards, "Should create at least %d shards when exceeding column limit: %s", tt.minShards, tt.description)

			// Verify each shard is valid and doesn't exceed the column limit
			verifyShardsWithLimit(t, ctx, bkt, shardCount, tt.maxNumColumns)
		})
	}
}

func Test_WithBloomFilterLabels(t *testing.T) {
	opts := DefaultConvertOpts

	WithBloomFilterLabels("__name__", "job", "instance")(&opts)

	require.Equal(t, []string{"__name__", "job", "instance"}, opts.bloomfilterLabels)

	bloomFilterCols := opts.buildBloomfilterColumns()
	require.Len(t, bloomFilterCols, 3)

	WithBloomFilterLabels()(&opts)
	require.Empty(t, opts.bloomfilterLabels)

	bloomFilterCols = opts.buildBloomfilterColumns()
	require.Empty(t, bloomFilterCols)
}

func readSeries(t *testing.T, shard storage.ParquetShard) ([]labels.Labels, [][]chunks.Meta, error) {
	ctx := context.Background()
	lr := parquet.NewGenericReader[any](shard.LabelsFile().WithContext(ctx))
	cr := parquet.NewGenericReader[any](shard.ChunksFile().WithContext(ctx))

	labelsBuff := make([]parquet.Row, 100)
	chunksBuff := make([]parquet.Row, 100)
	rLbls := make([]labels.Labels, 0, 100)
	rChunks := make([][]chunks.Meta, 0, 100)
	dec := schema.NewPrometheusParquetChunksDecoder(chunkenc.NewPool())
	for {
		nl, _ := lr.ReadRows(labelsBuff)
		if nl == 0 {
			break
		}

		nc, _ := cr.ReadRows(chunksBuff)

		if nc != nl {
			return nil, nil, fmt.Errorf("unexpected number of rows read: %d, expected %d", nl, nc)
		}
		s, _, err := rowToSeries(t, lr.Schema(), dec, labelsBuff[:nl])
		if err != nil {
			return nil, nil, err
		}
		_, c, err := rowToSeries(t, cr.Schema(), dec, chunksBuff[:nl])
		if err != nil {
			return nil, nil, err
		}
		rLbls = append(rLbls, s...)
		rChunks = append(rChunks, c...)
	}

	return rLbls, rChunks, nil
}

func rowToSeries(t *testing.T, s *parquet.Schema, dec *schema.PrometheusParquetChunksDecoder, rows []parquet.Row) ([]labels.Labels, [][]chunks.Meta, error) {
	cols := s.Columns()
	b := labels.NewScratchBuilder(10)
	series := make([]labels.Labels, len(rows))
	chunksMetas := make([][]chunks.Meta, len(rows))
	expectedLblsIdxs := []int{}
	foundLblsIdxs := []int{}
	for i, row := range rows {
		b.Reset()
		expectedLblsIdxs = expectedLblsIdxs[:0]
		foundLblsIdxs = foundLblsIdxs[:0]
		for colIdx, colVal := range row {
			col := cols[colIdx][0]
			label, ok := schema.ExtractLabelFromColumn(col)
			if ok {
				// Only include label columns that have actual values (not null/empty)
				// This matches what's stored in s_col_indexes - only labels present in the series
				if colVal.IsNull() {
					continue
				}
				b.Add(label, colVal.String())
				// Look up the ColumnIndex from the schema (same as when writing)
				lc, ok := s.Lookup(col)
				if !ok {
					return nil, nil, fmt.Errorf("column %s not found in schema", col)
				}
				foundLblsIdxs = append(foundLblsIdxs, lc.ColumnIndex)
			}

			if schema.IsDataColumn(col) && dec != nil {
				c, err := dec.Decode(colVal.ByteArray(), 0, math.MaxInt64)
				if err != nil {
					return nil, nil, err
				}
				chunksMetas[i] = append(chunksMetas[i], c...)
			}

			if col == schema.ColIndexesColumn {
				lblIdx, err := schema.DecodeUintSlice(colVal.ByteArray())
				if err != nil {
					return nil, nil, err
				}
				expectedLblsIdxs = lblIdx
			}
		}
		series[i] = b.Labels()
		slices.Sort(foundLblsIdxs)
		require.Equal(t, expectedLblsIdxs, foundLblsIdxs)
	}

	return series, chunksMetas, nil
}
