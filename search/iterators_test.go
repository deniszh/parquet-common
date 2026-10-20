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
	"fmt"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
)

func iteratorTestPage(t testing.TB, dictionary bool, count int, prefix string) parquet.Page {
	t.Helper()
	var node parquet.Node = parquet.String()
	if dictionary {
		node = parquet.Encoded(node, &parquet.RLEDictionary)
	}
	sc := parquet.NewSchema("labels", parquet.Group{"value": parquet.Optional(node)})
	var buf bytes.Buffer
	w := parquet.NewWriter(&buf, sc, parquet.PageBufferSize(1<<24))
	rows := make([]parquet.Row, count)
	for i := range rows {
		if i%7 == 0 {
			rows[i] = parquet.Row{parquet.NullValue().Level(0, 0, 0)}
		} else {
			rows[i] = parquet.Row{parquet.ValueOf(fmt.Sprintf("%s-%06d", prefix, i)).Level(0, 1, 0)}
		}
	}
	_, err := w.WriteRows(rows)
	require.NoError(t, err)
	require.NoError(t, w.Close())
	f, err := parquet.OpenFile(bytes.NewReader(buf.Bytes()), int64(buf.Len()))
	require.NoError(t, err)
	pages := f.RowGroups()[0].ColumnChunks()[0].Pages()
	t.Cleanup(func() { require.NoError(t, pages.Close()) })
	pg, err := pages.ReadPage()
	require.NoError(t, err)
	return pg
}

func TestPageValueIteratorReset(t *testing.T) {
	var it PageValueIterator
	var retained parquet.Value
	for _, dict := range []bool{true, true, false, true} {
		pg := iteratorTestPage(t, dict, 513, fmt.Sprint(dict))
		want := make([]parquet.Value, pg.NumValues())
		n, err := pg.Values().ReadValues(want)
		require.True(t, err == nil || err == io.EOF)
		require.Equal(t, len(want), n)
		it.Reset(pg)
		for j := range want {
			require.True(t, it.Next())
			got := it.At()
			require.True(t, parquet.Equal(want[j], got), "row %d: %v != %v", j, want[j], got)
			if retained.IsNull() && !got.IsNull() {
				retained = got
			}
		}
		require.False(t, it.Next())
		require.NoError(t, it.Err())
	}
	require.Equal(t, "true-000001", retained.String())
	// Skip and reset must not reuse values from the previous dictionary.
	it.Reset(iteratorTestPage(t, true, 513, "next"))
	require.Equal(t, int64(2), it.Skip(2))
	require.True(t, it.Next())
	require.Equal(t, "next-000002", it.At().String())
}

func TestSymbolTableReusesRowBuffer(t *testing.T) {
	pg := iteratorTestPage(t, true, 513, "symbol")
	var st SymbolTable
	st.Reset(pg)
	capacity := cap(st.syms)
	require.Zero(t, testing.AllocsPerRun(10, func() { st.Reset(pg) }))
	require.Equal(t, capacity, cap(st.syms))
	require.Zero(t, testing.AllocsPerRun(10, func() { st.ResetWithRange(pg, 1, 10) }))
	require.Equal(t, capacity, cap(st.syms))
	for i := 1; i < 10; i++ {
		if i%7 == 0 {
			require.True(t, st.Get(i).IsNull())
		} else {
			require.Equal(t, fmt.Sprintf("symbol-%06d", i), st.Get(i).String())
		}
	}
}

func BenchmarkPageValueIteratorSparseDictionary(b *testing.B) {
	pg := iteratorTestPage(b, true, 65536, "symbol")
	b.ReportAllocs()
	b.ResetTimer()
	var it PageValueIterator
	for b.Loop() {
		it.Reset(pg)
		it.Skip(1)
		if !it.Next() {
			b.Fatal(it.Err())
		}
		_ = it.At()
	}
}

func TestPageValueIteratorBoundsSymbolCache(t *testing.T) {
	var it PageValueIterator
	pg := iteratorTestPage(t, true, 2048, "first")
	it.Reset(pg)
	for it.Next() {
		_ = it.At()
	}
	require.Len(t, it.cachedSymbols, maxCachedPageSymbols)
	it.err = io.ErrUnexpectedEOF
	it.Reset(iteratorTestPage(t, true, 2, "second"))
	require.Empty(t, it.cachedSymbols)
	require.NoError(t, it.Err())
	require.True(t, it.Next())
	require.True(t, it.At().IsNull())
	require.True(t, it.Next())
	require.Equal(t, "second-000001", it.At().String())
}

func BenchmarkPageValueIteratorDenseDictionary(b *testing.B) {
	pg := iteratorTestPage(b, true, 65536, "symbol")
	b.ReportAllocs()
	b.ResetTimer()
	var it PageValueIterator
	for b.Loop() {
		it.Reset(pg)
		for it.Next() {
			_ = it.At()
		}
		if err := it.Err(); err != nil {
			b.Fatal(err)
		}
	}
}
