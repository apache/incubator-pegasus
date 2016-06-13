using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.InteropServices;

namespace dsn.dev.csharp
{
    public abstract class ReplicatedServiceAppType1 : ServiceApp
    {
        //
        // for stateful apps with layer 2 support
        //
        public virtual ErrorCode Checkpoint() { return ErrorCode.ERR_NOT_IMPLEMENTED; }

        public virtual ErrorCode CheckpointAsync() { return ErrorCode.ERR_NOT_IMPLEMENTED; }

        //
        // prepare an app-specific learning request (on learner, to be sent to learneee
        // and used by method get_checkpoint), so that the learning process is more efficient
        //
        // return value:
        //   0 - it is unnecessary to prepare a earn request
        //   <= capacity - learn request is prepared ready
        //   > capacity - buffer is not enough, caller should allocate a bigger buffer and try again
        //
        public virtual int PrepareGetCheckpoint(IntPtr buffer, int capacity) { return 0; }

        // 
        // Learn [start, infinite) from remote replicas (learner)
        //
        // Must be thread safe.
        //
        // The learned checkpoint can be a complete checkpoint (0, infinite), or a delta checkpoint
        // [start, infinite), depending on the capability of the underlying implementation.
        // 
        // Note the files in learn_state are copied from dir /replica@remote/data to dir /replica@local/learn,
        // so when apply the learned file state, make sure using learn_dir() instead of data_dir() to get the
        // full path of the files.
        //
        // the given state is a plain buffer where applications should fill it carefully for all the learn-state
        // into this buffer, with the total written size in total-learn-state-size.
        // if the capcity is not enough, applications should return ERR_CAPACITY_EXCEEDED and put required
        // buffer size in total-learn-state-size field, and our replication frameworks will retry with this new size
        //
        public abstract ErrorCode GetCheckpoint(
            Int64 start,
            IntPtr learn_request,
            int learn_request_size,
            ///* inout */ dsn_app_learn_state& state,
            IntPtr state_buffer,
            int state_capacity
            );

        //
        // [DSN_CHKPT_LEARN]
        // after learn the state from learner, apply the learned state to the local app
        //
        // Or,
        //
        // [DSN_CHKPT_COPY]
        // when an app only implement synchonous checkpoint, the primary replica
        // needs to copy checkpoint from secondaries instead of
        // doing checkpointing by itself, in order to not stall the normal
        // write operations.
        //
        // Postconditions:
        // * after apply_checkpoint() done, last_committed_decree() == last_durable_decree()
        // 
        public abstract ErrorCode ApplyCheckpoint(dsn_app_learn_state state, dsn_chkpt_apply_mode mode);
        
        private static int app_checkpoint(IntPtr app_handle)
        {
            var sapp = (((GCHandle)app_handle).Target as ServiceApp) as ReplicatedServiceAppType1;
            return sapp.Checkpoint();
        }

        private static int app_checkpoint_async(IntPtr app_handle)
        {
            var sapp = (((GCHandle)app_handle).Target as ServiceApp) as ReplicatedServiceAppType1;
            return sapp.CheckpointAsync();
        }

        private static int app_prepare_get_checkpoint(IntPtr app_handle, IntPtr buffer, int capacity)
        {
            var sapp = (((GCHandle)app_handle).Target as ServiceApp) as ReplicatedServiceAppType1;
            return sapp.PrepareGetCheckpoint(buffer, capacity);
        }

        private static int app_get_checkpoint(
            IntPtr app_handle,
            Int64 start_decree,
            IntPtr learn_request,
            int learn_request_size,
            ///* inout */ dsn_app_learn_state* state,
            IntPtr state_buffer,
            int state_capacity
            )
        {
            var sapp = (((GCHandle)app_handle).Target as ServiceApp) as ReplicatedServiceAppType1;
            return sapp.GetCheckpoint(start_decree, learn_request, learn_request_size, state_buffer, state_capacity);
        }

        private static int app_apply_checkpoint(IntPtr app_handle, dsn_app_learn_state state, dsn_chkpt_apply_mode mode)
        {
            var sapp = (((GCHandle)app_handle).Target as ServiceApp) as ReplicatedServiceAppType1;
            return sapp.ApplyCheckpoint(state, mode);
        }

        public static void RegisterAppWithType1ReplicationSupport<T>(string type_name)
            where T : ReplicatedServiceAppType1, new()
        {
            //dsn_app app;
            //memset(&app, 0, sizeof(app));
            //app.mask = DSN_L2_REPLICATION_APP_TYPE_1;
            //strncpy(app.type_name, type_name, sizeof(app.type_name));
            //app.layer1.create = service_app::app_create<TServiceApp>;
            //app.layer1.start = service_app::app_start;
            //app.layer1.destroy = service_app::app_destroy;

            //app.layer2_apps_type_1.chkpt = replicated_service_app_type_1::app_checkpoint;
            //app.layer2_apps_type_1.chkpt_async = replicated_service_app_type_1::app_checkpoint_async;
            //app.layer2_apps_type_1.checkpoint_get_prepare = replicated_service_app_type_1::app_prepare_get_checkpoint;
            //app.layer2_apps_type_1.chkpt_get = replicated_service_app_type_1::app_get_checkpoint;
            //app.layer2_apps_type_1.chkpt_apply = replicated_service_app_type_1::app_apply_checkpoint;

            //dsn_register_app(&app);
        }
    }
}
