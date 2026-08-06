// Copyright 2026 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// String values that collide with YAML reserved words (null, true,
// false, ...) must round-trip through the export unchanged. The export
// path passes scalar strings through yaml.Marshal precisely so these
// values are quoted on the way out.
func TestYAMLMarshalQuotesReservedWords(t *testing.T) {
	cases := map[string]string{
		"null":  `"null"`,
		"true":  `"true"`,
		"false": `"false"`,
		"hello": "hello",
	}
	for input, expected := range cases {
		buf, err := yaml.Marshal(input)
		require.NoError(t, err)
		got := strings.TrimRight(string(buf), "\n")
		require.Equal(t, expected, got, "input %q", input)
	}
}

func TestFormatArrayElement_Scalar(t *testing.T) {
	out, err := formatArrayElement("hello")
	require.NoError(t, err)
	require.Equal(t, "    - hello\n", out)
}

func TestFormatArrayElement_ObjectRoundtrip(t *testing.T) {
	// An export-shaped property value: array of objects.
	values := []any{
		map[string]any{"group_name": "producer_upload", "target_reserved": 2},
		map[string]any{"group_name": "consumer_fetch", "target_reserved": 2},
	}

	doc := "some_array_property:\n"
	for _, v := range values {
		s, err := formatArrayElement(v)
		require.NoError(t, err)
		doc += s
	}

	// The output must be parseable YAML that reconstitutes the original
	// array-of-objects shape (not an array of strings).
	var parsed map[string]any
	require.NoError(t, yaml.Unmarshal([]byte(doc), &parsed))

	got, ok := parsed["some_array_property"].([]any)
	require.True(t, ok, "expected []any, got %T", parsed["some_array_property"])
	require.Len(t, got, 2)

	first, ok := got[0].(map[string]any)
	require.True(t, ok, "expected map[string]any, got %T", got[0])
	require.Equal(t, "producer_upload", first["group_name"])
	require.Equal(t, 2, first["target_reserved"])
}

// JSON decode produces float64 for numbers; YAML decode produces int.
// logicalEqual must treat them as equal when they're integral and equal
// so the export → import round-trip on array-of-object properties
// reports "no changes" instead of flagging spurious deltas.
func TestLogicalEqual_JSONvsYAMLNumberTypes(t *testing.T) {
	fromJSON := []any{
		map[string]any{"group_name": "producer_upload", "target_reserved": float64(2)},
	}
	fromYAML := []any{
		map[string]any{"group_name": "producer_upload", "target_reserved": 2},
	}
	require.True(t, logicalEqual(fromJSON, fromYAML))
}

func TestLogicalEqual_DetectsActualDifferences(t *testing.T) {
	a := []any{map[string]any{"group_name": "producer_upload", "target_reserved": 2}}
	b := []any{map[string]any{"group_name": "producer_upload", "target_reserved": 3}}
	require.False(t, logicalEqual(a, b))
}
