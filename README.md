# kronos-scheduler

🚀 一个强大、灵活、可复用的 Go 任务调度框架，基于 robfig/cron/v3 构建

## ✨ 特性

### 核心功能

- 🕐 **Cron 表达式调度**：支持秒级精度的 Cron 表达式
- ⚡ **手动触发执行**：随时手动执行任何已注册的Job
- 📅 **定时执行**：XXL-JOB 风格的定时执行（指定时间+重复次数）
- 🔄 **任务补偿机制**：服务重启后自动补偿错过的任务
- 🔒 **重叠执行防止**：防止同一任务并发执行
- ⏱️ **超时控制**：支持任务执行超时设置
- 🔁 **自动重试**：任务失败后自动重试
- 📊 **执行记录追踪**：记录任务执行历史

### 架构特性

- 📦 **接口化设计**：所有组件都基于接口，易于扩展和测试
- 🗄️ **多存储后端**：支持内存、数据库（GORM）存储
- 🔴 **分布式就绪**：基于 Redis Pub/Sub 的分布式事件通知
- 🧩 **模块化**：job、repository、pubsub、logger 等模块独立可复用
- 🔌 **私有库集成**：使用 go-cachex、go-logger、go-toolbox等成熟组件

## 架构设计

### 整体架构

```mermaid
graph TB
    subgraph "应用层"
        App[应用程序]
    end
    
    subgraph "调度器核心"
        Scheduler["CronScheduler\n调度器"]
        Parser["CronParser\n表达式解析"]
        Chain["JobChain\n装饰器链"]
        Protector["JobProtector\n熔断保护"]
    end
    
    subgraph "任务层"
        Job["Job Interface\n任务接口"]
        BaseJob["BaseJob\n基础实现"]
        ShardingJob["ShardingJob\n分片任务"]
    end
    
    subgraph "存储层"
        JobRepo["JobConfigRepository\n配置存储"]
        SnapshotRepo["SnapshotRepository\n快照存储"]
        CacheRepo["CacheRepository\n缓存"]
    end
    
    subgraph "事件层"
        PubSub["PubSub\n发布订阅"]
        LocalPS["LocalPubSub\n本地实现"]
        RedisPS["RedisPubSub\n分布式实现"]
    end
    
    subgraph "基础设施"
        Redis[(Redis)]
        Database[(Database)]
    end
    
    App --> Scheduler
    Scheduler --> Parser
    Scheduler --> Chain
    Scheduler --> Protector
    Scheduler --> Job
    Job --> BaseJob
    Job --> ShardingJob
    
    Scheduler --> JobRepo
    Scheduler --> SnapshotRepo
    Scheduler --> CacheRepo
    Scheduler --> PubSub
    
    PubSub --> LocalPS
    PubSub --> RedisPS
    
    JobRepo --> Database
    SnapshotRepo --> Database
    CacheRepo --> Redis
    RedisPS --> Redis
    
    style Scheduler fill:#4CAF50
    style Job fill:#2196F3
    style PubSub fill:#FF9800
```

### 分布式架构

```mermaid
graph TB
    subgraph "节点 1"
        S1[Scheduler 1]
        J1[Jobs]
    end
    
    subgraph "节点 2"
        S2[Scheduler 2]
        J2[Jobs]
    end
    
    subgraph "节点 3"
        S3[Scheduler 3]
        J3[Jobs]
    end
    
    subgraph "共享基础设施"
        Redis["Redis\n• PubSub 事件\n• 分布式锁\n• 节点注册"]
        DB[("Database\n• 任务配置\n• 执行快照")]
    end
    
    S1 --> J1
    S2 --> J2
    S3 --> J3
    
    S1 <--> Redis
    S2 <--> Redis
    S3 <--> Redis
    
    S1 <--> DB
    S2 <--> DB
    S3 <--> DB
    
    style Redis fill:#FF5252
    style DB fill:#2196F3
```

### 任务执行流程

```mermaid
sequenceDiagram
    participant App as 应用
    participant Scheduler as 调度器
    participant Chain as 装饰器链
    participant Protector as 保护器
    participant Job as 任务
    participant Repo as 存储
    participant PubSub as 事件
    
    App->>Scheduler: RegisterJob(job, config)
    Scheduler->>Repo: 保存配置
    Scheduler->>PubSub: 发布配置更新事件
    
    Note over Scheduler: 到达执行时间
    
    Scheduler->>Chain: 执行装饰器链
    Chain->>Protector: 熔断检查
    Protector->>Job: Execute(ctx)
    Job-->>Protector: 执行结果
    Protector-->>Chain: 处理结果
    Chain-->>Scheduler: 返回结果
    
    Scheduler->>Repo: 保存执行快照
    Scheduler->>PubSub: 发布执行完成事件
```

### 事件驱动架构

```mermaid
graph LR
    subgraph "发布者"
        S1[Scheduler 1]
        S2[Scheduler 2]
    end
    
    subgraph "事件总线"
        PS["PubSub\n事件中心"]
    end
    
    subgraph "订阅者"
        S3[Scheduler 3]
        S4[Scheduler 4]
        Monitor[监控系统]
    end
    
    S1 -->|配置更新| PS
    S1 -->|任务执行| PS
    S2 -->|任务删除| PS
    S2 -->|节点心跳| PS
    
    PS -->|订阅事件| S3
    PS -->|订阅事件| S4
    PS -->|订阅事件| Monitor
    
    style PS fill:#FF9800
```

## 快速开始

### 安装

```bash
go get github.com/kamalyes/kronos-scheduler
```

### 基础示例

```go
package main

import (
    "context"
    "fmt"
    
    "github.com/kamalyes/go-config/pkg/jobs"
    "github.com/kamalyes/kronos-scheduler/scheduler"
    "github.com/kamalyes/kronos-scheduler/job"
    "github.com/kamalyes/kronos-scheduler/pubsub"
)

// 1. 定义任务
type HelloJob struct {
    *job.BaseJob
}

func (j *HelloJob) Execute(ctx context.Context) error {
    fmt.Println("Hello, Scheduler!")
    return nil
}

func main() {
    // 2. 创建 PubSub
    ps := pubsub.NewLocalPubSub()
    defer ps.Close()
    
    // 3. 创建调度器
    sched := scheduler.NewCronScheduler(
        scheduler.WithPubSub(ps),
    )
    
    // 4. 注册任务
    sched.RegisterJob(&HelloJob{
        BaseJob: job.NewBaseJob("hello", "示例任务"),
    }, jobs.TaskCfg{
        CronSpec: "0 * * * * *", // 每分钟执行
        Enabled:  true,
    })
    
    // 5. 启动
    sched.Start()
    defer sched.Stop()
    
    select {}
}
```

更多示例请查看 [快速开始文档](./docs/QUICKSTART.md)。

## 核心概念

### 模块组成

```mermaid
graph LR
    A[Job 任务] --> B[Scheduler 调度器]
    C[Repository 存储] --> B
    D[PubSub 事件] --> B
    E[Logger 日志] --> B
    
    B --> F[装饰器链]
    B --> G[任务保护器]
    B --> H[执行快照]
    
    style B fill:#4CAF50
```

### Job 接口

```go
type Job interface {
    Name() string                       // 任务唯一标识
    Description() string                // 任务描述  
    Execute(ctx context.Context) error // 执行逻辑
}
```

### 任务配置

```go
type TaskCfg struct {
    CronSpec       string   // Cron 表达式
    Enabled        bool     // 是否启用
    Timeout        int      // 超时（秒）
    MaxRetries     int      // 最大重试次数
    OverlapPrevent bool     // 防止重叠执行
    Dependencies   []string // 任务依赖
    Priority       int      // 优先级
}
```

详细说明请查看 [核心概念文档](./docs/CONCEPTS.md)。

## 高级特性

### 分布式调度

```mermaid
sequenceDiagram
    participant S1 as Scheduler 1
    participant Redis as Redis
    participant S2 as Scheduler 2
    
    S1->>Redis: 获取分布式锁
    Redis-->>S1: 锁获取成功
    S1->>S1: 执行任务
    S1->>Redis: 发布执行完成事件
    Redis->>S2: 广播事件
    S2->>S2: 更新本地状态
```

### 任务依赖 (DAG)

```mermaid
graph TD
    A[Job A] --> B[Job B]
    A --> C[Job C]
    B --> D[Job D]
    C --> D
    
    style A fill:#4CAF50
    style D fill:#2196F3
```

### 任务分片

```mermaid
graph LR
    Job[大数据任务] --> S1[分片 1]
    Job --> S2[分片 2]
    Job --> S3[分片 3]
    Job --> S4[分片 N]
    
    S1 --> R[结果汇总]
    S2 --> R
    S3 --> R
    S4 --> R
```

更多高级特性请查看 [高级特性文档](./docs/ADVANCED.md)。

## 文档导航

📚 **完整文档**

- [快速开始](./docs/QUICKSTART.md) - 安装和基础使用
- [核心概念](./docs/CONCEPTS.md) - Job、Schedule、装饰器等
- [高级特性](./docs/ADVANCED.md) - 分布式、DAG、分片等
- [PubSub 使用](./PUBSUB_USAGE.md) - 事件系统详解
- [迁移指南](./MIGRATION_COMPLETE.md) - 从 cron/v3 迁移
- [Syncx 集成](./SYNCX_INTEGRATION.md) - go-toolbox 集成

## 技术栈

| 组件 | 说明 |
|------|------|
| **调度引擎** | 基于 `robfig/cron/v3` |
| **存储** | GORM (MySQL/PostgreSQL/SQLite) |
| **缓存** | Redis (go-cachex) |
| **事件** | Redis Pub/Sub |
| **日志** | go-logger |
| **工具** | go-toolbox (重试、熔断、队列等) |

## 项目结构

```
kronos-scheduler/
├── docs/              # 📚 文档目录
│   ├── QUICKSTART.md  # 快速开始
│   ├── CONCEPTS.md    # 核心概念
│   └── ADVANCED.md    # 高级特性
├── job/               # 🔧 任务接口和实现
├── scheduler/         # ⚙️  核心调度器
├── repository/        # 💾 存储层
├── pubsub/            # 📡 事件发布订阅
├── models/            # 📊 数据模型
└── logger/            # 📝 日志适配器
```

## 贡献

欢迎贡献代码、报告问题或提出建议！

## 许可证

MIT License - 详见 [LICENSE](./LICENSE) 文件
