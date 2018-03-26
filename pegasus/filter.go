package pegasus

type FilterType int

const (
	FilterTypeNoFilter      FilterType = 0
	FilterTypeMatchAnywhere            = 1
	FilterTypeMatchPrefix              = 2
	FilterTypeMatchPostfix             = 3
)

type Filter struct {
	Type    FilterType
	Pattern []byte
}
