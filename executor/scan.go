package executor

import (
	"context"
	"fmt"
	"time"

	"github.com/XiaoMi/pegasus-go-client/pegasus"
)

// ScanCommand wraps scan command input.
type ScanCommand struct {
	HashKey string

	// optional
	From, To                 *string
	Prefix, Suffix, Contains *string
}

// IterateAll iterates over the table according to the command.
func (s *ScanCommand) IterateAll(rootCtx *Context) error {
	hashkey, err := rootCtx.HashKeyEnc.EncodeAll(s.HashKey)
	if err != nil {
		return err
	}

	var startSortKey, stopSortKey, filterPattern []byte
	var filterType pegasus.FilterType

	if s.From != nil {
		startSortKey, err = rootCtx.SortKeyEnc.EncodeAll(*s.From)
		if err != nil {
			return fmt.Errorf("invalid startSortKey: %s", err)
		}
	}
	if s.To != nil {
		stopSortKey, err = rootCtx.SortKeyEnc.EncodeAll(*s.To)
		if err != nil {
			return fmt.Errorf("invalid stopSortKey: %s", err)
		}
	}

	var filterStr *string
	if s.Prefix != nil {
		filterStr = s.Prefix
		filterType = pegasus.FilterTypeMatchPrefix
	}
	if s.Suffix != nil {
		filterStr = s.Suffix
		filterType = pegasus.FilterTypeMatchPostfix
	}
	if s.Contains != nil {
		filterStr = s.Contains
		filterType = pegasus.FilterTypeMatchAnywhere
	}
	if filterStr != nil {
		filterPattern, err = rootCtx.SortKeyEnc.EncodeAll(*filterStr)
		if err != nil {
			return fmt.Errorf("invalid filter: %s", err)
		}
	}

	sopts := &pegasus.ScannerOptions{
		BatchSize: 5,
		SortKeyFilter: pegasus.Filter{
			Type:    filterType,
			Pattern: filterPattern,
		},
		// TODO(wutao): provide options
		StartInclusive: true,
		StopInclusive:  true,
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	scanner, err := rootCtx.UseTable.GetScanner(ctx, hashkey, startSortKey, stopSortKey, sopts)
	if err != nil {
		return err
	}
	return iterateAllWithScanner(rootCtx, scanner)
}

// Validate if ScanCommand is valid.
func (s *ScanCommand) Validate() error {
	cnt := 0
	if s.Prefix != nil {
		cnt++
	}
	if s.Suffix != nil {
		cnt++
	}
	if s.Contains != nil {
		cnt++
	}
	if cnt > 1 {
		return fmt.Errorf("should specify only one of prefix|suffix|contains")
	}
	return nil
}

// iterateAllWithScanner prints all entries owned by scanner.
func iterateAllWithScanner(rootCtx *Context, scanner pegasus.Scanner) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	for {
		completed, hashKey, sortKey, value, err := scanner.Next(ctx)
		if err != nil {
			return err
		}
		if completed {
			break
		}
		err = printPegasusRecord(rootCtx, hashKey, sortKey, value)
		if err != nil {
			return err
		}
	}
	return nil
}
