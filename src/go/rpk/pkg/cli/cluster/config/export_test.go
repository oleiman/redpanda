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
