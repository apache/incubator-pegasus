package cmd

import (
	"github.com/desertbit/grumble"
	"github.com/pegasus-kv/admin-cli/executor"
	"github.com/pegasus-kv/admin-cli/shell"
)

func init() {
	shell.AddCommand(&grumble.Command{
		Name: "add-compaction-operation",
		Help: "add compaction operation and the corresponding rules",
		Flags: func(f *grumble.Flags) {
			/**
			 *	operations
			 **/
			f.String("o", "operation-type", "", "operation type, for example: delete/update-ttl")
			// update ttl operation
			f.String("u", "ttl-type", "", "update ttl operation type, for example: from_now/from_current/timestamp")
			f.Uint("v", "time-value", 0, "time value")
			/**
			 *  rules
			 **/
			// hashkey filter
			f.StringL("hashkey-pattern", "", "hash key pattern")
			f.StringL("hashkey-match", "anywhere", "hash key's match type, for example: anywhere/prefix/postfix")
			// sortkey filter
			f.StringL("sortkey-pattern", "", "sort key pattern")
			f.StringL("sortkey-match", "anywhere", "sort key's match type, for example: anywhere/prefix/postfix")
			// ttl filter
			f.Int64L("start-ttl", -1, "ttl filter, start ttl")
			f.Int64L("stop-ttl", -1, "ttl filter, stop ttl")
		},
		Run: shell.RequireUseTable(func(c *shell.Context) error {
			var params = &executor.CompactionParams{
				OperationType:  c.Flags.String("operation-type"),
				UpdateTTLType:  c.Flags.String("ttl-type"),
				TimeValue:      c.Flags.Uint("time-value"),
				HashkeyPattern: c.Flags.String("hashkey-pattern"),
				HashkeyMatch:   c.Flags.String("hashkey-match"),
				SortkeyPattern: c.Flags.String("sortkey-pattern"),
				SortkeyMatch:   c.Flags.String("sortkey-match"),
				StartTTL:       c.Flags.Int64("start-ttl"),
				StopTTL:        c.Flags.Int64("stop-ttl"),
			}
			return executor.SetCompaction(
				pegasusClient,
				c.UseTable,
				params)
		}),
	})
}
