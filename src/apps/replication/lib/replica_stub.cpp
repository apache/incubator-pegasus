/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "replica.h"
#include "replica_stub.h"
#include "mutation_log.h"
#include "mutation.h"
#include "replication_failure_detector.h"
#include "rpc_replicated.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.stub"

namespace dsn { namespace replication {

using namespace dsn::service;

replica_stub::replica_stub(replica_state_subscriber subscriber /*= nullptr*/, bool is_long_subscriber/* = true*/)
    : serverlet("replica_stub"), _replicas_lock(true)
{
    _replica_state_subscriber = subscriber;
    _is_long_subscriber = is_long_subscriber;
    _failure_detector = nullptr;
    _state = NS_Disconnected;
}

replica_stub::~replica_stub(void)
{
    close();
}

void replica_stub::initialize(bool clear/* = false*/)
{
    replication_options opts;
    opts.initialize();
    initialize(opts, clear);
}

void replica_stub::initialize(const replication_options& opts, bool clear/* = false*/)
{
    error_code err = ERR_OK;
    //zauto_lock l(_replicas_lock);

    // init dirs
    set_options(opts);
    _dir = _options.working_dir;
    
    if (clear)
    {
		if (!dsn::utils::filesystem::remove_path(_dir))
		{
			dassert("Fail to remove %s.", _dir.c_str());
		}
    }

	if (!dsn::utils::filesystem::create_directory(_dir))
	{
		dassert(false, "Fail to create directory %s.", _dir.c_str());
	}

	std::string temp;
	if (!dsn::utils::filesystem::get_absolute_path(_dir, temp))
	{
		dassert(false, "Fail to get absolute path from %s.", _dir.c_str());
	}
	_dir = temp;
    std::string log_dir = _dir + "/log";
	if (!dsn::utils::filesystem::create_directory(log_dir))
	{
		dassert(false, "Fail to create directory %s.", log_dir.c_str());
	}

    // init rps
    replicas rps;
	std::vector<std::string> dir_list;

	if (!dsn::utils::filesystem::get_subdirectories(dir(), dir_list, false))
	{
		dassert(false, "Fail to get files in %s.", dir().c_str());
	}

	for (auto& name : dir_list)
	{
		if (name.length() >= 4 &&
			(name.substr(name.length() - strlen("log")) == "log" ||
				name.substr(name.length() - strlen(".err")) == ".err")
			)
			continue;

		auto r = replica::load(this, name.c_str(), true);
		if (r != nullptr)
		{
			ddebug("%u.%u @ %s: load replica success with durable/commit decree = %llu / %llu from '%s'",
				r->get_gpid().app_id, r->get_gpid().pidx,
                primary_address().to_string(),
				r->last_durable_decree(),
                r->last_committed_decree(),
				name.c_str()
				);
			rps[r->get_gpid()] = r;
		}
	}
	dir_list.clear();

    // init logs when is_shared
    if (_options.log_enable_shared_prepare)
    {
        _log = new mutation_log(
            opts.log_buffer_size_mb,
            opts.log_pending_max_ms,
            opts.log_file_size_mb,
            opts.log_batch_write
            );
        err = _log->initialize(log_dir.c_str());
        dassert(err == ERR_OK, "");
        err = _log->replay(
            [&rps](mutation_ptr& mu)
        {
            auto it = rps.find(mu->data.header.gpid);
            if (it != rps.end())
            {
                it->second->replay_mutation(mu);
            }
        }
        );

        if (err != ERR_OK)
        {
            derror(
                "%s: replication log replay failed, err %s, clear all logs ...",
                primary_address().to_string(),
                err.to_string()
                );

            // restart log service
            _log->close();

            std::string err_log_path = log_dir + ".err";
            if (utils::filesystem::directory_exists(err_log_path))
                utils::filesystem::remove_path(err_log_path);

            utils::filesystem::rename_path(log_dir, err_log_path);
            utils::filesystem::create_directory(log_dir);

            _log = new mutation_log(
                opts.log_buffer_size_mb,
                opts.log_pending_max_ms,
                opts.log_file_size_mb,
                opts.log_batch_write
                );
            auto lerr = _log->initialize(log_dir.c_str());
            dassert(lerr == ERR_OK, "");
        }
    }
    else
    {
        _log = nullptr;
    }

    for (auto it = rps.begin(); it != rps.end(); it++)
    {
        it->second->reset_prepare_list_after_replay();

        if (err == ERR_OK)
        {
            dwarn(
                "%u.%u @ %s: global log initialized, durable = %lld, committed = %llu, maxpd = %llu, ballot = %llu",
                it->first.app_id, it->first.pidx,
                primary_address().to_string(),
                it->second->last_durable_decree(),
                it->second->last_committed_decree(),
                it->second->max_prepared_decree(),
                it->second->get_ballot()
                );

            it->second->set_inactive_state_transient(true);
        }
        else
        {
            dwarn(
                "%u.%u @ %s: global log initialized with log error, durable = %lld, committed = %llu, maxpd = %llu, ballot = %llu",
                it->first.app_id, it->first.pidx,
                primary_address().to_string(),
                it->second->last_durable_decree(),
                it->second->last_committed_decree(),
                it->second->max_prepared_decree(),
                it->second->get_ballot()
                );

            it->second->set_inactive_state_transient(false);
        }
    }

    // gc
    if (false == _options.gc_disabled)
    {
        _gc_timer_task = tasking::enqueue(
            LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
            this,
            &replica_stub::on_gc,
            0,
            random32(0, _options.gc_interval_ms),
            _options.gc_interval_ms
            );
    }

    // start log serving    
    if (_log != nullptr)
    {
        multi_partition_decrees init_max_decrees; // for log truncate
        for (auto it = rps.begin(); it != rps.end(); it++)
        {
            init_max_decrees[it->second->get_gpid()] = it->second->max_prepared_decree();
        }
        err = _log->start_write_service(init_max_decrees, _options.staleness_for_commit);
        dassert(err == ERR_OK, "");
    }

    // attach rps
    _replicas = std::move(rps);

    // start timer for configuration sync
    if (!_options.config_sync_disabled)
    {
        _config_sync_timer_task = tasking::enqueue(
            LPC_QUERY_CONFIGURATION_ALL,
            this,
            &replica_stub::query_configuration_by_node,
            0, 
            0,
            _options.config_sync_interval_ms
            );
    }
    
    // init livenessmonitor
    dassert (NS_Disconnected == _state, "");
    if (_options.fd_disabled == false)
    {
        _failure_detector = new replication_failure_detector(this, _options.meta_servers);
        err = _failure_detector->start(
            _options.fd_check_interval_seconds,
            _options.fd_beacon_interval_seconds,
            _options.fd_lease_seconds,
            _options.fd_grace_seconds
            );
        dassert(err == ERR_OK, "FD start failed, err = %s", err.to_string());

        _failure_detector->register_master(_failure_detector->current_server_contact());
    }
    else
    {
        _state = NS_Connected;
    }
}

replica_ptr replica_stub::get_replica(global_partition_id gpid, bool new_when_possible, const char* app_type)
{
    zauto_lock l(_replicas_lock);
    auto it = _replicas.find(gpid);
    if (it != _replicas.end())
        return it->second;
    else
    {
        if (!new_when_possible)
            return nullptr;
        else
        {
            dassert (app_type, "");
            replica* rep = replica::newr(this, app_type, gpid);
            if (rep != nullptr) 
            {
                add_replica(rep);
            }
            return rep;
        }
    }
}

replica_ptr replica_stub::get_replica(int32_t app_id, int32_t partition_index)
{
    global_partition_id gpid;
    gpid.app_id = app_id;
    gpid.pidx = partition_index;
    return get_replica(gpid);
}

void replica_stub::on_client_write(dsn_message_t request)
{
    write_request_header hdr;
    ::unmarshall(request, hdr);    
    
    replica_ptr rep = get_replica(hdr.gpid);
    if (rep != nullptr)
    {
        rep->on_client_write(dsn_task_code_from_string(hdr.code.c_str(), TASK_CODE_INVALID), request);
    }
    else
    {
        response_client_error(request, ERR_OBJECT_NOT_FOUND.get());
    }
}

void replica_stub::on_client_read(dsn_message_t request)
{
    read_request_header req;
    ::unmarshall(request, req);

    replica_ptr rep = get_replica(req.gpid);
    if (rep != nullptr)
    {
        rep->on_client_read(req, request);
    }
    else
    {
        response_client_error(request, ERR_OBJECT_NOT_FOUND.get());
    }
}

void replica_stub::on_config_proposal(const configuration_update_request& proposal)
{
    if (!is_connected()) return;

    replica_ptr rep = get_replica(proposal.config.gpid, proposal.type == CT_ASSIGN_PRIMARY, proposal.config.app_type.c_str());
    if (rep == nullptr)
    {
        if (proposal.type == CT_ASSIGN_PRIMARY)
        {
            begin_open_replica(proposal.config.app_type, proposal.config.gpid);
        }   
        else if (proposal.type == CT_UPGRADE_TO_PRIMARY)
        {
            remove_replica_on_meta_server(proposal.config);
        }
    }

    if (rep != nullptr)
    {
        rep->on_config_proposal((configuration_update_request&)proposal);
    }
}

void replica_stub::on_query_decree(const query_replica_decree_request& req, /*out*/ query_replica_decree_response& resp)
{
    replica_ptr rep = get_replica(req.gpid);
    if (rep != nullptr)
    {
        resp.err = ERR_OK;
        if (PS_POTENTIAL_SECONDARY == rep->status())
        {
            resp.last_decree = 0;
        }
        else
        {
            resp.last_decree = rep->last_committed_decree();
            // TODO: use the following to alleviate data lost
            //resp.last_decree = rep->last_prepared_decree();
        }
    }
    else
    {
        resp.err = ERR_OBJECT_NOT_FOUND;
        resp.last_decree = 0;
    }
}

void replica_stub::on_prepare(dsn_message_t request)
{
    global_partition_id gpid;
    ::unmarshall(request, gpid);    
    replica_ptr rep = get_replica(gpid);
    if (rep != nullptr)
    {
        rep->on_prepare(request);
    }
    else
    {
        prepare_ack resp;
        resp.gpid = gpid;
        resp.err = ERR_OBJECT_NOT_FOUND;
        reply(request, resp);
    }
}

void replica_stub::on_group_check(const group_check_request& request, /*out*/ group_check_response& response)
{
    if (!is_connected()) return;

    replica_ptr rep = get_replica(request.config.gpid, request.config.status == PS_POTENTIAL_SECONDARY, request.app_type.c_str());
    if (rep != nullptr)
    {
        rep->on_group_check(request, response);
    }
    else 
    {
        if (request.config.status == PS_POTENTIAL_SECONDARY)
        {
            std::shared_ptr<group_check_request> req(new group_check_request);
            *req = request;

            begin_open_replica(request.app_type, request.config.gpid, req);
            response.err = ERR_OK;
            response.learner_signature = 0;
        }
        else
        {
            response.err = ERR_OBJECT_NOT_FOUND;
        }
    }
}

void replica_stub::on_learn(dsn_message_t msg)
{
    learn_request request;
    ::unmarshall(msg, request);

    replica_ptr rep = get_replica(request.gpid);
    if (rep != nullptr)
    {
        rep->on_learn(msg, request);
    }
    else
    {
        learn_response response;
        response.err = ERR_OBJECT_NOT_FOUND;
        reply(msg, response);
    }
}

void replica_stub::on_learn_completion_notification(const group_check_response& report)
{
    replica_ptr rep = get_replica(report.gpid);
    if (rep != nullptr)
    {
        rep->on_learn_completion_notification(report);
    }
    else
    {
        report.err.end_tracking();
    }
}

void replica_stub::on_add_learner(const group_check_request& request)
{
    replica_ptr rep = get_replica(request.config.gpid, true, request.app_type.c_str());
    if (rep != nullptr)
    {
        rep->on_add_learner(request);
    }
    else
    {
        std::shared_ptr<group_check_request> req(new group_check_request);
        *req = request;
        begin_open_replica(request.app_type, request.config.gpid, req);
    }
}

void replica_stub::on_remove(const replica_configuration& request)
{
    replica_ptr rep = get_replica(request.gpid);
    if (rep != nullptr)
    {
        rep->on_remove(request);
    }
}

void replica_stub::query_configuration_by_node()
{
    if (_state == NS_Disconnected)
    {
        return;
    }

    if (_config_query_task != nullptr)
    {
        _config_query_task->cancel(false);
    }

    dsn_message_t msg = dsn_msg_create_request(RPC_CM_QUERY_NODE_PARTITIONS, 0, 0);

    configuration_query_by_node_request req;
    req.node = primary_address();
    ::marshall(msg, req);

    rpc_address target(_failure_detector->get_servers());
    _config_query_task = rpc::call(
        target,
        msg,
        this,
        std::bind(&replica_stub::on_node_query_reply, this, 
            std::placeholders::_1, 
            std::placeholders::_2, 
            std::placeholders::_3
            )
        );
}

void replica_stub::on_meta_server_connected()
{
    ddebug(
        "%s: meta server connected",
        primary_address().to_string()
        );

    zauto_lock l(_replicas_lock);
    if (_state == NS_Disconnected)
    {
        _state = NS_Connecting;
        query_configuration_by_node();
    }
}

void replica_stub::on_node_query_reply(error_code err, dsn_message_t request, dsn_message_t response)
{
    ddebug(
        "%s: node view replied, err = %s",
        primary_address().to_string(),
        err.to_string()
        );    

    if (err != ERR_OK)
    {
        zauto_lock l(_replicas_lock);
        if (_state == NS_Connecting)
        {
            query_configuration_by_node();
        }
    }
    else
    {
        zauto_lock l(_replicas_lock);
        if (_state == NS_Connecting)
        {
            _state = NS_Connected;
        }

        // DO NOT UPDATE STATE WHEN DISCONNECTED
        if (_state != NS_Connected)
            return;
        
        configuration_query_by_node_response resp;
        ::unmarshall(response, resp);

        if (resp.err != ERR_OK)
            return;
        
        replicas rs = _replicas;
        for (auto it = resp.partitions.begin(); it != resp.partitions.end(); it++)
        {
            rs.erase(it->gpid);
            tasking::enqueue(
                LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                this,
                std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
                gpid_to_hash(it->gpid)
                );
        }

        // for rps not exist on meta_servers
        for (auto it = rs.begin(); it != rs.end(); it++)
        {
            tasking::enqueue(
                LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                this,
                std::bind(&replica_stub::on_node_query_reply_scatter2, this, this, it->first),
                gpid_to_hash(it->first)
                );
        }
    }
}

void replica_stub::set_meta_server_connected_for_test(const configuration_query_by_node_response& resp)
{
    zauto_lock l(_replicas_lock);
    dassert (_state != NS_Connected, "");
    _state = NS_Connected;

    for (auto it = resp.partitions.begin(); it != resp.partitions.end(); it++)
    {
        tasking::enqueue(
            LPC_QUERY_NODE_CONFIGURATION_SCATTER,
            this,
            std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
            gpid_to_hash(it->gpid)
            );
    }
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on replica_stub::close
void replica_stub::on_node_query_reply_scatter(replica_stub_ptr this_, const partition_configuration& config)
{
    replica_ptr replica = get_replica(config.gpid);
    if (replica != nullptr)
    {
        replica->on_config_sync(config);
    }
    else
    {
        ddebug(
            "%u.%u @ %s: replica not exists on replica server, remove it from meta server",
            config.gpid.app_id, config.gpid.pidx,
            primary_address().to_string()
            );

        if (config.primary == primary_address())
        {
            remove_replica_on_meta_server(config);
        }
    }
}

void replica_stub::on_node_query_reply_scatter2(replica_stub_ptr this_, global_partition_id gpid)
{
    replica_ptr replica = get_replica(gpid);
    if (replica != nullptr && replica->status() != PS_POTENTIAL_SECONDARY)
    {
        ddebug(
            "%u.%u @ %s: replica not exists on meta server, removed",
            gpid.app_id, gpid.pidx,
            primary_address().to_string()
            );
        replica->update_local_configuration_with_no_ballot_change(PS_ERROR);
    }
}

void replica_stub::remove_replica_on_meta_server(const partition_configuration& config)
{
    dsn_message_t msg = dsn_msg_create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION, 0, 0);

    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    request->config = config;
    request->config.ballot++;        
    request->node = primary_address();
    request->type = CT_DOWNGRADE_TO_INACTIVE;

    if (primary_address() == config.primary)
    {
        request->config.primary.set_invalid();        
    }
    else if (replica_helper::remove_node(primary_address(), request->config.secondaries))
    {
    }
    else
    {
        return;
    }

    ::marshall(msg, *request);

    rpc_address target(_failure_detector->get_servers());
    rpc::call(
        _failure_detector->get_servers(),
        msg,
        nullptr,
        nullptr
        );
}

void replica_stub::on_meta_server_disconnected()
{
    ddebug(
        "%s: meta server disconnected",
        primary_address().to_string()
        );
    zauto_lock l(_replicas_lock);
    if (NS_Disconnected == _state)
        return;

    _state = NS_Disconnected;

    for (auto it = _replicas.begin(); it != _replicas.end(); it++)
    {
        tasking::enqueue(
            LPC_CM_DISCONNECTED_SCATTER,
            this,
            std::bind(&replica_stub::on_meta_server_disconnected_scatter, this, this, it->first),
            gpid_to_hash(it->first)
            );
    }
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on replica_stub::close
void replica_stub::on_meta_server_disconnected_scatter(replica_stub_ptr this_, global_partition_id gpid)
{
    {
        zauto_lock l(_replicas_lock);
        if (_state != NS_Disconnected)
            return;
    }

    replica_ptr replica = get_replica(gpid);
    if (replica != nullptr)
    {
        replica->on_meta_server_disconnected();
    }
}

void replica_stub::response_client_error(dsn_message_t request, int error)
{
    reply(request, error);
}

void replica_stub::init_gc_for_test()
{
    dassert (_options.gc_disabled, "");

    _gc_timer_task = tasking::enqueue(
        LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
        this,
        &replica_stub::on_gc,
        0,
        _options.gc_interval_ms
        );
}

void replica_stub::on_gc()
{
    replicas rs;
    {
        zauto_lock l(_replicas_lock);
        rs = _replicas;
    }

    // gc prepare log
    if (_log != nullptr)
    {
        multi_partition_decrees durable_decrees, max_seen_decrees;
        for (auto it = rs.begin(); it != rs.end(); it++)
        {
            durable_decrees[it->first] = it->second->last_durable_decree();
            max_seen_decrees[it->first] = it->second->max_prepared_decree();
        }
        _log->garbage_collection(durable_decrees, max_seen_decrees);
    }
    
    // gc commit log
    if (_options.log_enable_private_commit)
    {
        for (auto it = rs.begin(); it != rs.end(); it++)
        {
            it->second->commit_log()->garbage_collection_when_as_commit_logs(
                it->first,
                it->second->last_durable_decree()
                );
        }
    }

    // gc on-disk rps
	std::vector<std::string> sub_list;
	if (!dsn::utils::filesystem::get_subdirectories(_dir, sub_list, false))
	{
		dassert(false, "Fail to get subdirectories in %s.", _dir.c_str());
	}
	std::string ext = ".err";
	for (auto& fpath : sub_list)
	{
		auto&& name = dsn::utils::filesystem::get_file_name(fpath);
		if ((name.length() > ext.length())
			&& (name.compare((name.length() - ext.length()), std::string::npos, ext) == 0)
			)
		{
			time_t mt;
			if (!dsn::utils::filesystem::last_write_time(fpath, mt))
			{
				dassert(false, "Fail to get last write time of %s.", fpath.c_str());
			}

			if (mt > ::time(0) + _options.gc_disk_error_replica_interval_seconds)
			{
				if (!dsn::utils::filesystem::remove_path(fpath))
				{
					dassert(false, "Fail to delete directory %s.", fpath.c_str());
				}
			}
		}
	}
	sub_list.clear();

#if 0
    boost::filesystem::directory_iterator endtr;
    for (boost::filesystem::directory_iterator it(dir());
        it != endtr;
        ++it)
    {
        auto name = it->path().filename().string();
        if (name.length() > strlen(".err") && name.substr() == ".err")
        {
            std::time_t mt = boost::filesystem::last_write_time(it->path());
            if (mt > time(0) + _options.gc_disk_error_replica_interval_seconds)
            {
                boost::filesystem::remove_all(_dir + "/" + name);
            }
        }
    }
#endif
}

::dsn::task_ptr replica_stub::begin_open_replica(const std::string& app_type, global_partition_id gpid, std::shared_ptr<group_check_request> req)
{
    _replicas_lock.lock();
    if (_replicas.find(gpid) != _replicas.end())
    {
        _replicas_lock.unlock();
        return nullptr;
    }        

    auto it = _opening_replicas.find(gpid);
    if (it != _opening_replicas.end())
    {
        _replicas_lock.unlock();
        return nullptr;
    }
    else 
    {
        auto it2 = _closing_replicas.find(gpid);
        if (it2 != _closing_replicas.end())
        {
            if (it2->second.second->status() == PS_INACTIVE 
                && it2->second.first->cancel(false))
            {
                replica_ptr r = it2->second.second;
                _closing_replicas.erase(it2);
                add_replica(r);

                // unlock here to avoid dead lock
                _replicas_lock.unlock();

                ddebug( "open replica which is to be closed '%s.%u.%u'", app_type.c_str(), gpid.app_id, gpid.pidx);

                if (req != nullptr)
                {
                    on_add_learner(*req);
                }
                return nullptr;
            }
            else 
            {
                _replicas_lock.unlock();
                dwarn( "open replica '%s.%u.%u' failed coz replica is under closing", 
                    app_type.c_str(), gpid.app_id, gpid.pidx);                
                return nullptr;
            }
        }
        else 
        {
            auto task = tasking::enqueue(LPC_OPEN_REPLICA, this, std::bind(&replica_stub::open_replica, this, app_type, gpid, req));
            _opening_replicas[gpid] = task;
            _replicas_lock.unlock();
            return task;
        }
    }
}

void replica_stub::open_replica(const std::string app_type, global_partition_id gpid, std::shared_ptr<group_check_request> req)
{
    char buffer[256];
    sprintf(buffer, "%u.%u.%s", gpid.app_id, gpid.pidx, app_type.c_str());

    std::string dr = dir() + "/" + buffer;

    dwarn("open replica '%s'", dr.c_str());

    replica_ptr rep = replica::load(this, dr.c_str(), true);
    if (rep == nullptr) rep = replica::newr(this, app_type.c_str(), gpid);
    dassert (rep != nullptr, "");
        
    {
        zauto_lock l(_replicas_lock);
        auto it = _replicas.find(gpid);
        dassert (it == _replicas.end(), "");
        add_replica(rep);
        _opening_replicas.erase(gpid);
    }

    if (nullptr != req)
    {
        rpc::call_one_way_typed(primary_address(), RPC_LEARN_ADD_LEARNER, *req, gpid_to_hash(req->config.gpid));
    }
}

::dsn::task_ptr replica_stub::begin_close_replica(replica_ptr r)
{
    zauto_lock l(_replicas_lock);

    // initialization is still ongoing
    if (nullptr == _failure_detector)
        return nullptr;

    if (remove_replica(r))
    {
        auto task = tasking::enqueue(LPC_CLOSE_REPLICA, this, 
            std::bind(&replica_stub::close_replica, this, r), 
            0, 
            r->status() == PS_ERROR ? 0 : _options.gc_memory_replica_interval_ms
            );
        _closing_replicas[r->get_gpid()] = std::make_pair(task, r);
        return task;
    }
    else
    {
        return nullptr;
    }
}

void replica_stub::close_replica(replica_ptr r)
{
    dwarn( "close replica '%s'", r->dir().c_str());

    r->close();

    {
        zauto_lock l(_replicas_lock);
        _closing_replicas.erase(r->get_gpid());
    }
}

void replica_stub::add_replica(replica_ptr r)
{
    zauto_lock l(_replicas_lock);
    _replicas[r->get_gpid()] = r;
}

bool replica_stub::remove_replica(replica_ptr r)
{
    zauto_lock l(_replicas_lock);
    if (_replicas.erase(r->get_gpid()) > 0)
    {
        return true;
    }
    else
        return false;
}

void replica_stub::notify_replica_state_update(const replica_configuration& config, bool isClosing)
{
    if (nullptr != _replica_state_subscriber)
    {
        if (_is_long_subscriber)
        {
            tasking::enqueue(LPC_REPLICA_STATE_CHANGE_NOTIFICATION, this, std::bind(_replica_state_subscriber, primary_address(), config, isClosing));
        }
        else
        {
            _replica_state_subscriber(primary_address(), config, isClosing);
        }
    }
}

void replica_stub::handle_log_failure(error_code err)
{
    // TODO:
}

void replica_stub::open_service()
{
    register_rpc_handler(RPC_REPLICATION_CLIENT_WRITE, "write", &replica_stub::on_client_write);
    register_rpc_handler(RPC_REPLICATION_CLIENT_READ, "read", &replica_stub::on_client_read);

    register_rpc_handler(RPC_CONFIG_PROPOSAL, "ProposeConfig", &replica_stub::on_config_proposal);

    register_rpc_handler(RPC_PREPARE, "prepare", &replica_stub::on_prepare);
    register_rpc_handler(RPC_LEARN, "Learn", &replica_stub::on_learn);
    register_rpc_handler(RPC_LEARN_COMPLETION_NOTIFY, "LearnNotify", &replica_stub::on_learn_completion_notification);
    register_rpc_handler(RPC_LEARN_ADD_LEARNER, "LearnAdd", &replica_stub::on_add_learner);
    register_rpc_handler(RPC_REMOVE_REPLICA, "remove", &replica_stub::on_remove);
    register_rpc_handler(RPC_GROUP_CHECK, "GroupCheck", &replica_stub::on_group_check);
    register_rpc_handler(RPC_QUERY_PN_DECREE, "query_decree", &replica_stub::on_query_decree);
}

void replica_stub::close()
{
    if (_config_sync_timer_task != nullptr)
    {
        _config_sync_timer_task->cancel(true);
        _config_sync_timer_task = nullptr;
    }

    if (_config_query_task != nullptr)
    {
        _config_query_task->cancel(true);
        _config_query_task = nullptr;
    }
    _state = NS_Disconnected;
    
    if (_gc_timer_task != nullptr)
    {
        _gc_timer_task->cancel(true);
        _gc_timer_task = nullptr;
    }
    
    {
        zauto_lock l(_replicas_lock);    
        while (_closing_replicas.empty() == false)
        {
            auto task = _closing_replicas.begin()->second.first;
            _replicas_lock.unlock();

            task->wait();

            _replicas_lock.lock();
            _closing_replicas.erase(_closing_replicas.begin());
        }

        while (_opening_replicas.empty() == false)
        {
            auto task = _opening_replicas.begin()->second;
            _replicas_lock.unlock();

            task->cancel(true);

            _replicas_lock.lock();
            _opening_replicas.erase(_opening_replicas.begin());
        }

        while (_replicas.empty() == false)
        {
            _replicas.begin()->second->close();
            _replicas.erase(_replicas.begin());
        }
    }
        
    if (_failure_detector != nullptr)
    {
        _failure_detector->stop();
        delete _failure_detector;
        _failure_detector = nullptr;
    }

    if (_log != nullptr)
    {
        _log->close();
        _log = nullptr;
    }
}

}} // namespace

