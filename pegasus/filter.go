package pegasus

import "github.com/XiaoMi/pegasus-go-client/idl/rrdb"

type FilterType int

const (
	FilterTypeNoFilter      = FilterType(rrdb.FilterType_FT_NO_FILTER)
	FilterTypeMatchAnywhere = FilterType(rrdb.FilterType_FT_MATCH_ANYWHERE)
	FilterTypeMatchPrefix   = FilterType(rrdb.FilterType_FT_MATCH_PREFIX)
	FilterTypeMatchPostfix  = FilterType(rrdb.FilterType_FT_MATCH_POSTFIX)
)

type Filter struct {
	Type    FilterType
	Pattern []byte
}
