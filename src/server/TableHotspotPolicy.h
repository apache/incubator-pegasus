#include "data_store.h"

namespace pegasus {
    namespace server {
        class Hotspot_policy
        {
        public:
            Hotspot_policy(Data_store *data_store){
                _data_store = data_store;
            }
            virtual double detect_hotspot_policy() = 0;
        private:
            Data_store *_data_store;
        };

        class Algo1 : public Hotspot_policy
        {
        public:
            explicit Algo1(Data_store *data_store):Hotspot_policy(data_store){};
            double detect_hotspot_policy()
            {
                return 0;
            }
        };

        class Algo2 : public Hotspot_policy
        {
        public:
            explicit Algo2(Data_store *data_store):Hotspot_policy(data_store){};
            double detect_hotspot_policy()
            {
                return 0;
            }
        };


        class Hotpot_calculator
        {
        private:
            std::string _name;


        public:
            std::vector< std::vector<Data_store> > data_stores;

            Hotpot_calculator(const std::string &name,int app_size) : _name(name), data_stores(app_size){}

            void aggregate(const std::vector<row_data> *partitions) {
                for (int i=0;i<partitions->size();i++){
                    data_stores[i].push_back(partitions[i]);
                }
            }

        };
    }
}
