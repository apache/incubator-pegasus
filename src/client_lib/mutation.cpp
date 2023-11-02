/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <pegasus/client.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "../base/pegasus_utils.h"
#include "utils/enum_helper.h"

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
