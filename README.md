# webdav-sync

用 **Go（纯标准库，零第三方依赖）** 实现的 WebDAV → 本机同步工具。支持：

- **递归同步**：PROPFIND 逐层展开远端目录树，完整镜像目录结构
- **多任务**：单个 JSON 配置文件定义多个 `(远端 → 本地)` 同步任务，逐个执行
- **并发下载**：单任务内多线程并发下载，`--concurrency` 可调
- **增量同步**：按 远端大小 + mtime（或 etag）跳过未变更文件，重复运行只拉取增量
- **原子下载**：先写 `.part` 临时文件再重命名，中断安全；失败自动重试 3 次
- **可选清理**：`--delete` 可删除远端已不存在的本地文件
- **配置灵活**：命令行 / 环境变量 / JSON 文件；字符串支持 `${ENV}` 展开，便于容器与 cron

镜像已通过 GitHub Actions 自动构建并推送到 Docker Hub：`totootao/webdav-sync`。

## 命令行用法

```bash
# 单任务（命令行）
webdav-sync \
  --url https://dav.example.com/dav/docs \
  --local /data/docs \
  --username alice --password "$WEBDAV_PASSWORD" \
  --concurrency 8

# 多任务（JSON 配置文件）
webdav-sync --config config.json

# 先预览将要下载哪些文件（不实际下载）
webdav-sync --config config.json --dry-run

# 同步并清理远端已删除的本地文件
webdav-sync --url https://dav.example.com/dav/docs --local /data/docs --delete
```

环境变量等价写法：`WEBDAV_URL`、`WEBDAV_LOCAL_DIR`、`WEBDAV_USERNAME`、`WEBDAV_PASSWORD`、`SYNC_CONCURRENCY`。

### 参数

| 参数 | 说明 |
| --- | --- |
| `--config` | 多任务 JSON 配置（见 `config.example.json`） |
| `--url` / `--local` | 单任务的远端目录 URL 与本地目标目录 |
| `--username` / `--password` | 认证信息（也可用环境变量） |
| `--concurrency` | 并发下载数（默认 8） |
| `--delete` | 删除远端已不存在的本地文件 |
| `--no-verify-tls` | 跳过 TLS 证书校验（自签名证书场景） |
| `--dry-run` | 只打印将要下载的文件 |

## 配置文件（多任务）

```json
{
  "concurrency": 8,
  "delete": false,
  "tasks": [
    { "name": "docs",  "url": "https://dav.example.com/dav/docs",  "local": "/data/docs",  "username": "alice", "password": "${WEBDAV_PASSWORD}" },
    { "name": "photos","url": "https://dav.example.com/dav/photos","local": "/data/photos","concurrency": 16 }
  ]
}
```

- 顶层 `concurrency` / `delete` 为全局默认，可被单个任务覆盖。
- 字符串中的 `${VAR}` 会自动展开为环境变量（支持 `${VAR:-默认值}`）。

## Docker 用法

```bash
docker run --rm -v /path/local:/data totootao/webdav-sync \
  --url https://dav.example.com/dav/docs --local /data/docs \
  --username alice --password "$WEBDAV_PASSWORD"
```

搭配配置文件：

```bash
docker run --rm \
  -v /path/local:/data \
  -v /path/config.json:/config.json \
  totootao/webdav-sync --config /config.json
```

### 定时同步（cron 示例）

```cron
# 每天 03:00 同步
0 3 * * *  docker run --rm -v /data:/data totootao/webdav-sync --config /data/config.json >> /var/log/webdav-sync.log 2>&1
```

## 构建

```bash
go build -o webdav-sync .
./webdav-sync --help
```

## 说明

- 时间判定：优先用 `getetag`；其次用 `getlastmodified` 与本地 mtime 比较（容差 2 秒）。
- 仅同步文件内容，不依赖远端特定扩展属性；适用于 Nextcloud / ownCloud / OpenList / nginx-dav 等标准 WebDAV 服务。
