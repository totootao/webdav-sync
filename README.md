# webdav-sync

用 **Go（纯标准库，零第三方依赖）** 实现的 WebDAV → 本机同步工具。支持：

- **递归同步**：PROPFIND 逐层展开远端目录树，完整镜像目录结构
- **多任务**：单个 JSON 配置文件定义多个 `(远端 → 本地)` 同步任务，逐个执行
- **并发下载**：单任务内多线程并发下载，`--concurrency` 可调
- **增量同步**：按 远端大小 + mtime（或 etag）跳过未变更文件，重复运行只拉取增量
- **原子下载**：先写 `.part` 临时文件再重命名，中断安全
- **断点续传 / 抗抖动**：大文件以 8MiB 分块、走 HTTP Range 逐块下载，**单块失败仅重传该块**；连接中途断开（如 Alist 反代 Google Drive 等大文件场景常见的 `unexpected EOF`）也能自动续传直至完整；并带读取超时防假死
- **可选清理**：`--delete` 可删除远端已不存在的本地文件
- **配置灵活**：命令行 / 环境变量 / JSON 文件；字符串支持 `${ENV}` 展开，便于容器与 cron
- **Web 管理界面**：内置 Web UI，可视化增删改查多个同步任务、一键触发同步、查看实时日志与运行状态

镜像已通过 GitHub Actions 自动构建并推送到 Docker Hub：`totootao/webdav-sync`。

## Web 管理界面

内置一个零依赖的 Web UI，适合在服务器/NAS 上常驻，可视化地管理多个同步任务。

```bash
# 启动 Web 服务（默认端口 8080，配置持久化到 ./webdav-sync.json）
webdav-sync --web --addr :8080 --config ./webdav-sync.json

# 加 Basic Auth 保护界面（建议公网/共享环境必开）
webdav-sync --web --addr :8080 --config ./webdav-sync.json --web-auth admin:你的密码

# 每 3600 秒自动同步全部任务（可选）
webdav-sync --web --addr :8080 --config ./webdav-sync.json --interval 3600
```

打开浏览器访问 `http://<服务器>:8080` 即可：

- **任务列表**：展示每个任务的名称、远端 URL、本地目录、并发、状态（空闲/运行中/成功/失败）、上次运行时间与简要结果。
- **新增 / 编辑任务**：表单填写名称、URL、本地目录、账号密码、并发数、TLS 校验与删除策略；编辑时密码留空表示保留原密码。
- **一键同步**：点击「同步」立即在后台执行该任务（已运行则跳过）。
- **任务日志**：点击「日志」查看该任务最近一次运行的明细。
- **实时日志**：底部控制台通过 SSE 实时推送所有任务的运行日志。
- **全局设置**：统一设置默认并发数、默认删除策略、默认跳过 TLS 校验。

> 任务定义保存在 `--config` 指定的 JSON 文件中（密码以明文存储，请妥善保管该文件，并配合 `--web-auth` 与文件权限保护）。API 列表接口会对密码脱敏（`has_password` 标记是否存在密码）。

### Web 模式参数

| 参数 | 说明 |
| --- | --- |
| `--web` | 启动 Web 管理界面（与一次性 CLI 互斥） |
| `--addr` | Web 监听地址（默认 `:8080`） |
| `--web-auth` | 界面 Basic Auth，格式 `user:pass` |
| `--interval` | 自动同步间隔秒数（0=关闭），到点批量触发全部任务 |

### REST API（供自动化调用）

| 方法 & 路径 | 说明 |
| --- | --- |
| `GET /api/tasks` | 列出全部任务（密码脱敏） |
| `POST /api/tasks` | 新增任务（JSON body） |
| `PUT /api/tasks/{name}` | 更新任务（密码留空则保留） |
| `DELETE /api/tasks/{name}` | 删除任务 |
| `POST /api/tasks/{name}/run` | 后台触发同步 |
| `GET /api/tasks/{name}/logs` | 该任务最近一次运行日志 |
| `GET /api/config` · `PUT /api/config` | 读取/更新全局默认配置 |
| `GET /api/stream` | SSE 实时日志流 |

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

镜像默认以 **Web 管理界面** 启动（监听 `:8080`，配置存于 `/data/config.json`）：

```bash
docker run -d --name webdav-sync -p 8080:8080 \
  -v /path/local:/data \
  totootao/webdav-sync
# 然后浏览器打开 http://<服务器>:8080
```

挂载已有配置并加鉴权：

```bash
docker run -d --name webdav-sync -p 8080:8080 \
  -v /path/local:/data \
  -v /path/config.json:/data/config.json \
  totootao/webdav-sync --web --addr :8080 --config /data/config.json --web-auth admin:你的密码
```

一次性 CLI 同步（覆盖默认 CMD）：

```bash
docker run --rm -v /path/local:/data totootao/webdav-sync \
  --url https://dav.example.com/dav/docs --local /data/docs \
  --username alice --password "$WEBDAV_PASSWORD"
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
