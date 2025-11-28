package parser

import (
	"testing"
	"time"
)

// Sample wal2json payload for benchmarking
var sampleWal2JSONInsert = []byte(`{
	"xid": 12345,
	"timestamp": "2024-01-15T10:30:00.123456Z",
	"change": [{
		"kind": "insert",
		"schema": "public",
		"table": "users",
		"columnnames": ["id", "name", "email", "created_at", "is_active"],
		"columnvalues": [1, "Test User", "test@example.com", "2024-01-15T10:30:00Z", true]
	}]
}`)

var sampleWal2JSONUpdate = []byte(`{
	"xid": 12346,
	"timestamp": "2024-01-15T10:31:00.123456Z",
	"change": [{
		"kind": "update",
		"schema": "public",
		"table": "users",
		"columnnames": ["id", "name", "email", "updated_at"],
		"columnvalues": [1, "Updated User", "updated@example.com", "2024-01-15T10:31:00Z"],
		"oldkeys": {
			"keynames": ["id"],
			"keyvalues": [1]
		}
	}]
}`)

var sampleWal2JSONMultiChange = []byte(`{
	"xid": 12347,
	"timestamp": "2024-01-15T10:32:00.123456Z",
	"change": [
		{"kind": "insert", "schema": "public", "table": "users", "columnnames": ["id", "name"], "columnvalues": [1, "User 1"]},
		{"kind": "insert", "schema": "public", "table": "users", "columnnames": ["id", "name"], "columnvalues": [2, "User 2"]},
		{"kind": "insert", "schema": "public", "table": "users", "columnnames": ["id", "name"], "columnvalues": [3, "User 3"]},
		{"kind": "insert", "schema": "public", "table": "users", "columnnames": ["id", "name"], "columnvalues": [4, "User 4"]},
		{"kind": "insert", "schema": "public", "table": "users", "columnnames": ["id", "name"], "columnvalues": [5, "User 5"]}
	]
}`)

var sampleWal2JSONLarge = generateLargeWal2JSON()

func generateLargeWal2JSON() []byte {
	// Generate a wal2json payload with 20 columns
	return []byte(`{
		"xid": 12348,
		"timestamp": "2024-01-15T10:33:00.123456Z",
		"change": [{
			"kind": "insert",
			"schema": "public",
			"table": "large_table",
			"columnnames": ["col1", "col2", "col3", "col4", "col5", "col6", "col7", "col8", "col9", "col10", "col11", "col12", "col13", "col14", "col15", "col16", "col17", "col18", "col19", "col20"],
			"columnvalues": ["value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9", "value10", "value11", "value12", "value13", "value14", "value15", "value16", "value17", "value18", "value19", "value20"]
		}]
	}`)
}

// BenchmarkDecodeWal2JSONInsert benchmarks decoding a single insert
func BenchmarkDecodeWal2JSONInsert(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONInsert, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONUpdate benchmarks decoding an update with old keys
func BenchmarkDecodeWal2JSONUpdate(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONUpdate, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDecodeWal2JSONMultiChange benchmarks decoding multiple changes in one transaction
func BenchmarkDecodeWal2JSONMultiChange(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONMultiChange, nil)
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
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONLarge, nil)
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
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONInsert, filter)
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
		_, err := decodeWal2JSON(0x16B3748, sampleWal2JSONInsert, filter)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkToMap benchmarks the toMap helper function
func BenchmarkToMap(b *testing.B) {
	keys := []string{"id", "name", "email", "created_at", "updated_at", "is_active", "balance"}
	vals := []interface{}{1, "Test User", "test@example.com", time.Now(), time.Now(), true, 123.45}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = toMap(keys, vals)
	}
}

// BenchmarkToMapSmall benchmarks toMap with small inputs
func BenchmarkToMapSmall(b *testing.B) {
	keys := []string{"id"}
	vals := []interface{}{1}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = toMap(keys, vals)
	}
}
