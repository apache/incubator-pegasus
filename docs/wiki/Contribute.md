## License
rDSN is provided in the MIT open source license. 

## You are more than welcome to contribute

The beauty of open source world is that we can benefit from each other. We are more than glad to accept any contributions to the project, including but not limited to bug fixes and feature improvements. You can do contribution by submitting pull requests or sending us email at rdsn-support@googlegroups.com.

One first contribution is to adopt rDSN to develop your distributed systems, so as to leveraging all these tools rDSN provides, or embracing the replication framework provided by rDSN. Please try and share us your feedbacks so we can continuously improving the code.

Besides, we also believe that there are the following aspects that developers may easily contribute to benefit all other applications atop of rDSN. Welcome to submit pull requests and/or sending us emails for direct contribution and/or collaborations on these topics. For those stand-alone components, we can integrate their code into part of the rDSN source tree, or, we can also have dedicated wiki pages to list the URLs so people can easily find them. In both cases, developers contributing the components may need to take some time to react to the further issues or pull requests or anything else related to the correspondent components. 

#### More native runtime libraries

Currently, rDSN already provides a set of default native runtime components, and they are far from being perfect. We are looking for more component providers with higher performance and/or on new platforms (e.g., on mobile platforms), such as task queues, exclusive locks, reader-writer locks, semaphore, network libraries (e.g., on RDMA or new platforms), performance counter, logging system. Detailed interface requirements can be found in the source code (see <dsn/tool_api.h>), and they can be seamlessly integrated into rDSN as long as the interface fits.

#### More development and operation plug-ins

The tool API in rDSN exposes reliable monitoring and manipulation capability to all underlying plug-ins about how the tasks in the upper application work. We hope this capability can help other developers and researchers as well to develop more development and operation tools as rDSN plug-ins. After all, we started this project partially because we ourselves as tool developers on distributed systems have felt a lot of pain in figuring out reliable approaches to understand and/or manipulating the legacy distributed systems.

#### More runtime control policies

Besides development and operation tools, we also believe the taint analysis capability via state extension and join points in rDSN introduce certain opportunities in implementing and applying more interesting runtime control policies at the task and user request granularity, such as multi-tenant resource isolation and adaptive admission control.

#### More code templates for more automation

One goal of rDSN is to really eliminate the system complexities imposed by distributed systems so that application developers can focus on business logic complexities purely. Code generation is clearly one approach towards this goal, and it is great if people can work out more code templates on more tasks. 

#### Other possible opportunities

Any other thought, let us know! 

