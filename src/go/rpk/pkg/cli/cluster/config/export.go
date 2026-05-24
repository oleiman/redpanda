// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/redpanda-data/common-go/rpadmin"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/adminapi"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/spf13/afero"
	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

// formatArrayElement renders a single array element as a YAML list item
// with 4-space outer indentation. Scalars use fmt's default formatting;
// objects (maps) are marshaled via yaml so their fields land as nested
// keys rather than as a Go-literal "map[k:v]" string.
func formatArrayElement(v any) (string, error) {
	switch v.(type) {
	case map[string]any, []any:
		buf, err := yaml.Marshal(v)
		if err != nil {
			return "", err
		}
		lines := strings.Split(strings.TrimRight(string(buf), "\n"), "\n")
		var sb strings.Builder
		for i, line := range lines {
			if i == 0 {
				sb.WriteString("    - ")
			} else {
				sb.WriteString("      ")
			}
			sb.WriteString(line)
			sb.WriteByte('\n')
		}
		return sb.String(), nil
	default:
		return fmt.Sprintf("    - %v\n", v), nil
	}
}

func exportConfig(
	file *os.File, schema rpadmin.ConfigSchema, config rpadmin.Config, all bool,
) (err error) {
	// Present properties in alphabetical order, providing some pseudo-grouping based on common prefixes
	keys := make([]string, 0, len(schema))
	for k := range schema {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, name := range keys {
		// We exclude cluster_id from exported config to avoid accidental
		// duplication of the ID from one cluster to another
		if name == "cluster_id" {
			continue
		}
		meta := schema[name]
		curValue := config[name]

		// Deprecated settings are never shown to the user for editing
		if meta.Visibility == "deprecated" {
			continue
		}

		// Only show tunables if the user passed --all
		if meta.Visibility == "tunable" && !all {
			continue
		}

		var sb strings.Builder

		// Preface each property with a descriptive comment
		var commentTokens []string

		if len(meta.EnumValues) > 0 {
			commentTokens = append(commentTokens, fmt.Sprintf("one of %s", strings.Join(meta.EnumValues, ", ")))
		} else if meta.Example != "" {
			commentTokens = append(commentTokens, fmt.Sprintf("e.g. '%s'", meta.Example))
		}

		if meta.NeedsRestart {
			commentTokens = append(commentTokens, "restart required")
		}

		if meta.Nullable {
			commentTokens = append(commentTokens, "may be nil")
		}

		commentDetails := ""
		if len(commentTokens) > 0 {
			commentDetails = fmt.Sprintf(" (%s)", strings.Join(commentTokens, ", "))
		}
		fmt.Fprintf(&sb, "\n# %s%s\n", meta.Description, commentDetails)

		// Compose a YAML representation of the property: this is
		// done with simple prints rather than the yaml module, because
		// in either case we have to carefully format values.
		if meta.Type == "array" {
			switch x := curValue.(type) {
			case nil:
				fmt.Fprintf(&sb, "%s:", name)
			case []any:
				if len(x) > 0 {
					fmt.Fprintf(&sb, "%s:\n", name)
					for _, v := range x {
						elem, err := formatArrayElement(v)
						if err != nil {
							return fmt.Errorf("formatting %s element: %w", name, err)
						}
						sb.WriteString(elem)
					}
				} else {
					fmt.Fprintf(&sb, "%s: []", name)
				}
			default:
				out.Die("unexpected property value type: %s: %T", name, curValue)
			}
		} else {
			scalarVal := ""
			switch x := curValue.(type) {
			case float64:
				scalarVal = strconv.FormatFloat(x, 'f', -1, 64)
			case string:
				// yaml-quote strings that would otherwise be parsed as
				// reserved words (null, true, false, ...) on re-import.
				buf, err := yaml.Marshal(x)
				if err != nil {
					return fmt.Errorf("formatting %s value: %w", name, err)
				}
				scalarVal = strings.TrimRight(string(buf), "\n")
			case bool:
				scalarVal = strconv.FormatBool(x)
			case nil:
				// Leave scalarVal empty
			default:
				out.Die("unexpected property value type: %s: %T", name, curValue)
			}

			if len(scalarVal) > 0 {
				fmt.Fprintf(&sb, "%s: %s\n", name, scalarVal)
			} else {
				fmt.Fprintf(&sb, "%s:\n", name)
			}
		}

		_, err := file.Write([]byte(sb.String()))
		if err != nil {
			return err
		}
	}

	return nil
}

func newExportCommand(fs afero.Fs, p *config.Params) *cobra.Command {
	var filename string
	var all bool
	cmd := &cobra.Command{
		Use:   "export",
		Short: "Export cluster configuration",
		Long: `Export cluster configuration.

Writes out a YAML representation of the cluster configuration to a file,
suitable for editing and later applying with the corresponding 'import'
command.

By default, low level tunables are excluded: use the '--all' flag
to include all properties including these low level tunables.
`,
		Args: cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			p, err := p.LoadVirtualProfile(fs)
			out.MaybeDie(err, "rpk unable to load config: %v", err)
			config.CheckExitCloudAdmin(p)

			client, err := adminapi.NewClient(cmd.Context(), fs, p)
			out.MaybeDie(err, "unable to initialize admin client: %v", err)

			// GET the schema
			schema, err := client.ClusterConfigSchema(cmd.Context())
			out.MaybeDie(err, "unable to query config schema: %v", err)

			// GET current config
			var currentConfig rpadmin.Config
			currentConfig, err = client.Config(cmd.Context(), true)
			out.MaybeDie(err, "unable to query current config: %v", err)

			// Generate a yaml template for editing
			var file *os.File
			if filename == "" {
				file, err = os.CreateTemp("/tmp", "config_*.yaml")
				filename = "/tmp/config_*.yaml"
			} else {
				file, err = os.Create(filename)
			}

			out.MaybeDie(err, "unable to create file %q: %v", filename, err)
			err = exportConfig(file, schema, currentConfig, all)
			out.MaybeDie(err, "failed to write out config %q: %v", file.Name(), err)
			err = file.Close()
			fmt.Fprintf(os.Stderr, "Wrote configuration to file %q.\n", file.Name())
			out.MaybeDie(err, "error closing file %q: %v", file.Name(), err)
		},
	}

	cmd.Flags().StringVarP(&filename, "filename", "f", "", "path to file to export to, e.g. './config.yml'")
	cmd.Flags().BoolVar(&all, "all", false, "Include all properties, including tunables")

	return cmd
}
