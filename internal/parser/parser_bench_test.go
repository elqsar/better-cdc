package parser

import (
	"testing"
)

// Sample wal2json format-version 2 payloads for benchmarking

var sampleWal2JSONV2Begin = []byte(`{
	"action": "B",
	"xid": 12345,
	"timestamp": "2024-01-15 10:30:00.123456+00"
}`)

var sampleWal2JSONV2Insert = []byte(`{
	"action": "I",
	"xid": 12345,
	"timestamp": "2024-01-15 10:30:00.123456+00",
	"schema": "public",
	"table": "users",
	"columns": [
		{"name": "id", "type": "bigint", "value": 1},
		{"name": "name", "type": "text", "value": "Test User"},
		{"name": "email", "type": "text", "value": "test@example.com"},
		{"name": "created_at", "type": "timestamp with time zone", "value": "2024-01-15 10:30:00+00"},
		{"name": "is_active", "type": "boolean", "value": true}
	]
}`)

var sampleWal2JSONV2Update = []byte(`{
	"action": "U",
	"xid": 12346,
	"timestamp": "2024-01-15 10:31:00.123456+00",
	"schema": "public",
	"table": "users",
	"columns": [
		{"name": "id", "type": "bigint", "value": 1},
		{"name": "name", "type": "text", "value": "Updated User"},
		{"name": "email", "type": "text", "value": "updated@example.com"},
		{"name": "updated_at", "type": "timestamp with time zone", "value": "2024-01-15 10:31:00+00"}
	],
	"identity": [
		{"name": "id", "type": "bigint", "value": 1}
	]
}`)

var sampleWal2JSONV2Delete = []byte(`{
	"action": "D",
	"xid": 12347,
	"timestamp": "2024-01-15 10:32:00.123456+00",
	"schema": "public",
	"table": "users",
	"identity": [
		{"name": "id", "type": "bigint", "value": 1}
	]
}`)

var sampleWal2JSONV2Commit = []byte(`{
	"action": "C",
	"xid": 12345,
	"timestamp": "2024-01-15 10:30:00.123456+00"
}`)

var sampleWal2JSONV2Large = generateLargeWal2JSONV2()

func generateLargeWal2JSONV2() []byte {
	// Generate a wal2json format-version 2 payload with 20 columns
	return []byte(`{
		"action": "I",
		"xid": 12348,
		"timestamp": "2024-01-15 10:33:00.123456+00",
		"schema": "public",
		"table": "large_table",
		"columns": [
			{"name": "col1", "type": "text", "value": "value1"},
			{"name": "col2", "type": "text", "value": "value2"},
			{"name": "col3", "type": "text", "value": "value3"},
			{"name": "col4", "type": "text", "value": "value4"},
			{"name": "col5", "type": "text", "value": "value5"},
			{"name": "col6", "type": "text", "value": "value6"},
			{"name": "col7", "type": "text", "value": "value7"},
			{"name": "col8", "type": "text", "value": "value8"},
			{"name": "col9", "type": "text", "value": "value9"},
			{"name": "col10", "type": "text", "value": "value10"},
			{"name": "col11", "type": "text", "value": "value11"},
			{"name": "col12", "type": "text", "value": "value12"},
			{"name": "col13", "type": "text", "value": "value13"},
			{"name": "col14", "type": "text", "value": "value14"},
			{"name": "col15", "type": "text", "value": "value15"},
			{"name": "col16", "type": "text", "value": "value16"},
			{"name": "col17", "type": "text", "value": "value17"},
			{"name": "col18", "type": "text", "value": "value18"},
			{"name": "col19", "type": "text", "value": "value19"},
			{"name": "col20", "type": "text", "value": "value20"}
		]
	}`)
}

// BenchmarkDecodeWal2JSONBegin benchmarks decoding a begin message
func BenchmarkDecodeWal2JSONBegin(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Begin, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONInsert benchmarks decoding a single insert
func BenchmarkDecodeWal2JSONInsert(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Insert, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONUpdate benchmarks decoding an update with identity
func BenchmarkDecodeWal2JSONUpdate(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Update, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONDelete benchmarks decoding a delete
func BenchmarkDecodeWal2JSONDelete(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Delete, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONCommit benchmarks decoding a commit message
func BenchmarkDecodeWal2JSONCommit(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Commit, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONLarge benchmarks decoding a payload with many columns
func BenchmarkDecodeWal2JSONLarge(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Large, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONWithFilter benchmarks decoding with table filtering
func BenchmarkDecodeWal2JSONWithFilter(b *testing.B) {
	filter := map[string]struct{}{
		"public.users": {},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Insert, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONFilteredOut benchmarks decoding when table is filtered out
func BenchmarkDecodeWal2JSONFilteredOut(b *testing.B) {
	filter := map[string]struct{}{
		"public.other_table": {},
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONV2Insert, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPopulateMapFromColumns benchmarks the populateMapFromColumns helper function
func BenchmarkPopulateMapFromColumns(b *testing.B) {
	cols := []wal2JSONColumn{
		{Name: "id", Type: "bigint", Value: 1},
		{Name: "name", Type: "text", Value: "Test User"},
		{Name: "email", Type: "text", Value: "test@example.com"},
		{Name: "created_at", Type: "timestamp with time zone", Value: "2024-01-15 10:30:00+00"},
		{Name: "updated_at", Type: "timestamp with time zone", Value: "2024-01-15 10:30:00+00"},
		{Name: "is_active", Type: "boolean", Value: true},
		{Name: "balance", Type: "numeric", Value: 123.45},
	}
	m := make(map[string]interface{}, 16)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for k := range m {
			delete(m, k)
		}
		populateMapFromColumns(m, cols)
	}
}

// BenchmarkPopulateMapFromColumnsSmall benchmarks populateMapFromColumns with small inputs
func BenchmarkPopulateMapFromColumnsSmall(b *testing.B) {
	cols := []wal2JSONColumn{
		{Name: "id", Type: "bigint", Value: 1},
	}
	m := make(map[string]interface{}, 16)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		for k := range m {
			delete(m, k)
		}
		populateMapFromColumns(m, cols)
	}
}
