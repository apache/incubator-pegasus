// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <pegasus/client.h>
#include "../base/pegasus_utils.h"

using namespace ::dsn;

namespace pegasus {

void pegasus_client::mutations::set(const std::string &sort_key,
                                    const std::string &value,
                                    const int ttl_seconds)
{
    mutate mu;
    mu.operation = mutate::mutate_operation::MO_PUT;
    mu.sort_key = sort_key;
    mu.value = value;
    // set_expire_ts_seconds will be set when check_and_mutate() gets the mutations (by
    // calling get_mutations())
    mu.set_expire_ts_seconds = 0;
    mu_list.emplace_back(std::move(mu));
    if (ttl_seconds != 0) {
        ttl_list.emplace_back(std::make_pair(mu_list.size() - 1, ttl_seconds));
    }
}

void pegasus_client::mutations::set(std::string &&sort_key,
                                    std::string &&value,
                                    const int ttl_seconds)
{
    mutate mu;
    mu.operation = mutate::mutate_operation::MO_PUT;
    mu.sort_key = std::move(sort_key);
    mu.value = std::move(value);
    // set_expire_ts_seconds will be set when check_and_mutate() gets the mutations (by
    // calling get_mutations())
    mu.set_expire_ts_seconds = 0;
    mu_list.emplace_back(std::move(mu));
    if (ttl_seconds != 0) {
        ttl_list.emplace_back(std::make_pair(mu_list.size() - 1, ttl_seconds));
    }
}

void pegasus_client::mutations::del(const std::string &sort_key)
{
    mutate mu;
    mu.operation = mutate::mutate_operation::MO_DELETE;
    mu.sort_key = sort_key;
    mu.set_expire_ts_seconds = 0;
    mu_list.emplace_back(std::move(mu));
}

void pegasus_client::mutations::del(std::string &&sort_key)
{
    mutate mu;
    mu.operation = mutate::mutate_operation::MO_DELETE;
    mu.sort_key = std::move(sort_key);
    mu.set_expire_ts_seconds = 0;
    mu_list.emplace_back(std::move(mu));
}

void pegasus_client::mutations::get_mutations(std::vector<mutate> &mutations) const
{
    int current_time = pegasus::utils::epoch_now();
    mutations = mu_list;
    for (auto &pair : ttl_list) {
        mutations[pair.first].set_expire_ts_seconds = pair.second + current_time;
    }
}
}
