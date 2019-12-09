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
                //// need to implement finding max_qps
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
            Collection() {}
            void set_policy(Table_hotspot_policy *s) {
                _policy = s;
            }
            void load_stat(const table_stats *stats){
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