# cron-postgres

`cron-postgres` 是 `github.com/infrago/cron` 的 **PostgreSQL 驱动**。

## 包定位

- 类型：驱动
- 作用：把 `cron` 模块的统一接口落到 PostgreSQL 后端实现

## 快速接入

```go
import (
    _ "github.com/infrago/cron"
    _ "github.com/infrago/cron-postgres"
)
```

```toml
[cron]
driver = "postgres"
```

## `setting` 专用配置项

配置位置：`[cron].setting`

- `schema`
- `jobs_table`
- `logs_table`
- `locks_table`
- `dsn`
- `url`
- `host`
- `port`
- `username`
- `user`
- `password`
- `database`
- `dbname`
- `sslmode`

## 说明

- `setting` 仅对当前驱动生效，不同驱动键名可能不同
- 连接失败时优先核对 `setting` 中 host/port/认证/超时等参数
