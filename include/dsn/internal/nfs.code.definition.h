# pragma once
# include <dsn/service_api.h>
# include "nfs.types.h"


# define __TITLE__ "nfs"
# define MAXBUFSIZE 4096 // one round rpc buffer
# define MAXREQUESTCOUNT 10000 // for control client traffic
# define OUTOFDATE 10000 // time for close file
# define CLIENTPATH "D:/rdsn/tutorial/nfs_v3/client/mydir/"
# define SERVERPATH "D:/rdsn/tutorial/nfs_v3/server/"

namespace dsn { namespace service { 
	// define RPC task code for service 'nfs'
	DEFINE_TASK_CODE_RPC(RPC_NFS_V3_NFS_COPY, ::dsn::TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
	DEFINE_TASK_CODE_RPC(RPC_NFS_V3_NFS_GET_FILE_SIZE, ::dsn::TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
	// test timer task code
	DEFINE_TASK_CODE(LPC_NFS_V3_REQUEST_TIMER, ::dsn::TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
	DEFINE_TASK_CODE(LPC_NFS_V3_EXE_TIMER, ::dsn::TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

	DEFINE_TASK_CODE_AIO(LPC_NFS_READ, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
	DEFINE_TASK_CODE(LPC_NFS_FILE_CLOSE_TIMER, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

	DEFINE_TASK_CODE_AIO(LPC_NFS_WRITE, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

	DEFINE_TASK_CODE_AIO(LPC_NFS_COPY_FILE, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
} } 
