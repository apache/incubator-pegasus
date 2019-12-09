#include <dsn/perf_counter/perf_counter.h>
#include "info_collector.h"
#include "table_stats.h"

namespace pegasus {
namespace server {

    class Table_hotspot_policy {
        public:
            virtual double detect_hotspot_policy(const table_stats *stats) = 0;
        };

    class Algo1 : public Table_hotspot_policy {
        public:
            virtual double detect_hotspot_policy(const table_stats *stats) {
                return (stats->max_total_qps / std::max(stats->min_total_qps, 1.0));
            }
    };

    class Algo2 : public Table_hotspot_policy {
    public:
        virtual double detect_hotspot_policy(const table_stats *stats) {
            return (stats->max_total_cu / std::max(stats->min_total_cu, 1.0));
        }
    };

    class Collection {
        private:
            Table_hotspot_policy *_policy;
            const table_stats *_stats;
            const std::string _stat_name;
            const std::string _disc;
            double _ans;
        public:
            Collection(Table_hotspot_policy *s,const table_stats *stats) {
                _policy = s;
                _stats = stats;
            }
            void cal_policy(){
               _ans = _policy->detect_hotspot_policy(_stats);
            }
            const double get_ans(){
                return _ans;
            }
    };

}
}