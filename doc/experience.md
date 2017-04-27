* 每次重启，workerId将自增，内置实现为在启动时由数据库分配
* 默认单机支持8192个并发，如果需要更强的并发能力，水平扩展机器。如果是中心化方式，生产的速度将大于消费的速度，网络会成为瓶颈。
* ring的填充：BufferPaddingExecutor::paddingBuffer()中往ring中put



