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

	"github.com/parquet-go/parquet-go"

	"github.com/prometheus-community/parquet-common/storage"
)

// Cached files outlive the request that opened them. Load lazy indexes using
// the current query's context, rather than the file's original reader.
func offsetIndex(ctx context.Context, file storage.ParquetFileView, cc parquet.ColumnChunk) (parquet.OffsetIndex, error) {
	if c, ok := cc.(*parquet.FileColumnChunk); ok {
		return c.OffsetIndexFrom(file.WithContext(ctx))
	}
	return cc.OffsetIndex()
}

func columnIndex(ctx context.Context, file storage.ParquetFileView, cc parquet.ColumnChunk) (parquet.ColumnIndex, error) {
	if c, ok := cc.(*parquet.FileColumnChunk); ok {
		return c.ColumnIndexFrom(file.WithContext(ctx))
	}
	return cc.ColumnIndex()
}
