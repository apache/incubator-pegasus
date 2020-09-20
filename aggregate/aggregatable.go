package aggregate

var v1Tov2MetricsConversion = map[string]string{
	"replica*app.pegasus*get_qps":                                  "get_qps",
	"replica*app.pegasus*multi_get_qps":                            "multi_get_qps",
	"replica*app.pegasus*put_qps":                                  "put_qps",
	"replica*app.pegasus*multi_put_qps":                            "multi_put_qps",
	"replica*app.pegasus*remove_qps":                               "remove_qps",
	"replica*app.pegasus*multi_remove_qps":                         "multi_remove_qps",
	"replica*app.pegasus*incr_qps":                                 "incr_qps",
	"replica*app.pegasus*check_and_set_qps":                        "check_and_set_qps",
	"replica*app.pegasus*check_and_mutate_qps":                     "check_and_mutate_qps",
	"replica*app.pegasus*scan_qps":                                 "scan_qps",
	"replica*eon.replica*backup_request_qps":                       "backup_request_qps",
	"replica*app.pegasus*duplicate_qps":                            "duplicate_qps",
	"replica*app.pegasus*dup_shipped_ops":                          "dup_shipped_ops",
	"replica*app.pegasus*dup_failed_shipping_ops":                  "dup_failed_shipping_ops",
	"replica*app.pegasus*get_bytes":                                "get_bytes",
	"replica*app.pegasus*multi_get_bytes":                          "multi_get_bytes",
	"replica*app.pegasus*scan_bytes":                               "scan_bytes",
	"replica*app.pegasus*put_bytes":                                "put_bytes",
	"replica*app.pegasus*multi_put_bytes":                          "multi_put_bytes",
	"replica*app.pegasus*check_and_set_bytes":                      "check_and_set_bytes",
	"replica*app.pegasus*check_and_mutate_bytes":                   "check_and_mutate_bytes",
	"replica*app.pegasus*recent.read.cu":                           "recent_read_cu",
	"replica*app.pegasus*recent.write.cu":                          "recent_write_cu",
	"replica*app.pegasus*recent.expire.count":                      "recent_expire_count",
	"replica*app.pegasus*recent.filter.count":                      "recent_filter_count",
	"replica*app.pegasus*recent.abnormal.count":                    "recent_abnormal_count",
	"replica*eon.replica*recent.write.throttling.delay.count":      "recent_write_throttling_delay_count",
	"replica*eon.replica*recent.write.throttling.reject.count":     "recent_write_throttling_reject_count",
	"replica*app.pegasus*disk.storage.sst(MB)":                     "sst_storage_mb",
	"replica*app.pegasus*disk.storage.sst.count":                   "sst_count",
	"replica*app.pegasus*rdb.block_cache.hit_count":                "rdb_block_cache_hit_count",
	"replica*app.pegasus*rdb.block_cache.total_count":              "rdb_block_cache_total_count",
	"replica*app.pegasus*rdb.index_and_filter_blocks.memory_usage": "rdb_index_and_filter_blocks_mem_usage",
	"replica*app.pegasus*rdb.memtable.memory_usage":                "rdb_memtable_mem_usage",
	"replica*app.pegasus*rdb.estimate_num_keys":                    "rdb_estimate_num_keys",
	"replica*app.pegasus*rdb.bf_seek_negatives":                    "rdb_bf_seek_negatives",
	"replica*app.pegasus*rdb.bf_seek_total":                        "rdb_bf_seek_total",
	"replica*app.pegasus*rdb.bf_point_positive_true":               "rdb_bf_point_positive_true",
	"replica*app.pegasus*rdb.bf_point_positive_total":              "rdb_bf_point_positive_total",
	"replica*app.pegasus*rdb.bf_point_negatives":                   "rdb_bf_point_negatives",
}

var aggregatableSet = map[string]interface{}{
	"read_qps":    nil,
	"write_qps":   nil,
	"read_bytes":  nil,
	"write_bytes": nil,
}

// aggregatable returns whether the counter is to be aggregated on collector,
// including v1Tov2MetricsConversion and aggregatableSet.
func aggregatable(pc *partitionPerfCounter) bool {
	v2Name, found := v1Tov2MetricsConversion[pc.name]
	if found { // ignored
		pc.name = v2Name
		return true // listed above are all aggregatable
	}
	_, found = aggregatableSet[pc.name]
	return found
}
