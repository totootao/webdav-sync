# webdav-sync

用 **Go（纯标准库，零第三方依赖）** 实现的 WebDAV 双向同步工具。支持 **下载（pull，远端 → 本地）** 与 **上传（push，本地 → 远端）** 两个方向：

- **递归同步**：PROPFIND / 本地 WalkDir 逐层展开目录树，完整镜像目录结构
- **双向方向**：每个任务可选 `direction`：
  - `pull`（默认）：把远端 WebDAV 文件下载到本地
  - `push`：把本地目录文件上传到远端 WebDAV（自动创建远端目录、按大小增量、可选清理远端多余文件）
- **多任务**：单个 JSON 配置文件定义多个同步任务，逐个执行
- **并发传输**：单任务内多线程并发，`--concurrency` 可调
- **增量同步**：按 大小（或 etag/mtime）跳过未变更文件，重复运行只传输增量
- **原子下载**：先写 `.part` 临时文件再重命名，中断安全
- **断点续传 / 抗抖动（下载）**：大文件以 8MiB 分块、走 HTTP Range 逐块下载，**单块失败仅重传该块**；连接中途断开（如 Alist 反代 Google Drive 等大文件场景常见的 `unexpected EOF`）也能自动续传直至完整；并带读取超时防假死
- **流式上传（push）**：以 PUT 流式上传本地文件（带读取超时防假死），失败整段重试
- **可选清理**：`--delete` 可删除对端已不存在的文件（pull 删本地、push 删远端）
- **配置灵活**：命令行 / 环境变量 / JSON 文件；字符串支持 `${ENV}` 展开，便于容器与 cron
- **Web 管理界面**：内置 Web UI，可视化增删改查多个同步任务、一键触发同步、查看实时日志与运行状态；**手机端自适应**（响应式布局，窄屏自动卡片化）
- **独立 WebDAV 服务器档案**：可把 WebDAV 连接（URL/账号/密码/TLS）配置成可复用的「服务器档案」，支持**测试连接**验证可用性；任务既可**引用档案**（自动复用其连接，多任务共享同一服务器时免重复填凭据），也可**自带字段**兜底，二选一/互补

镜像已通过 GitHub Actions 自动构建并推送到 Docker Hub：`totootao/webdav-sync`。

## Web 管理界面

内置一个零依赖的 Web UI，适合在服务器/NAS 上常驻，可视化地管理多个同步任务。界面同时适配桌面与手机端（窄屏下表格自动转为卡片，按钮加大便于触控）。

```bash
# 启动 Web 服务（默认端口 8080，配置持久化到 ./webdav-sync.json）
webdav-sync --web --addr :8080 --config ./webdav-sync.json

# 加 Basic Auth 保护界面（建议公网/共享环境必开）
webdav-sync --web --addr :8080 --config ./webdav-sync.json --web-auth admin:你的密码

# 每 3600 秒自动同步全部任务（可选）
webdav-sync --web --addr :8080 --config ./webdav-sync.json --interval 3600
```

打开浏览器（手机/电脑皆可）访问 `http://<服务器>:8080` 即可。界面分两个标签页：

### 同步任务

- **任务列表**：展示名称、WebDAV（引用档案名或 URL）、本地目录、并发、状态（空闲/运行中/成功/失败）、上次运行时间与简要结果。
- **新增 / 编辑任务**：
  - 「WebDAV 服务器」下拉选择已配置的**服务器档案**（自动复用其连接，含密码），或选「独立填写」自行填写 URL/账号/密码/TLS。
  - 「同步方向」选择 `下载（WebDAV → 本地）` 或 `上传（本地 → WebDAV）`；上传时会自动在远端创建对应目录、按大小做增量、并在开启 `--delete` 时清理远端多余文件。
  - 引用档案的任务自身可只填名称、本地目录与并发，无需重复录入凭据。
  - 编辑时密码留空表示保留原密码。
  - 「测试连接」按钮可即时验证当前填写/引用的连接是否可用。
- **一键同步**：点击「同步」立即在后台执行该任务（已运行则跳过）。
- **任务日志**：点击「日志」查看该任务最近一次运行的明细。
- **实时日志**：底部控制台通过 SSE 实时推送所有任务的运行日志。
- **全局设置**：统一设置默认并发数、默认删除策略、默认跳过 TLS 校验。

### WebDAV 服务器

- **档案列表**：列出全部服务器档案（名称、URL、用户名、TLS、是否设置密码），密码已脱敏。
- **新增 / 编辑档案**：填写名称、URL、账号、密码、TLS 策略；编辑时密码留空表示保留。
- **测试连接**：每个档案和编辑表单都有「测试」按钮，对远端根目录做一次 PROPFIND 验证可达性与认证；成功/失败即时提示。
- **删除**：删除档案后，引用它的任务会退化为「自带字段」（若任务本身未填 URL 则运行时会报错，请提前补填）。

> 任务与服务器档案都保存在 `--config` 指定的 JSON 文件中（密码以明文存储，请妥善保管该文件，并配合 `--web-auth` 与文件权限保护）。列表/读取接口会对密码脱敏（`has_password` 标记是否存在密码）。

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
| `GET /api/servers` | 列出全部服务器档案（密码脱敏） |
| `POST /api/servers` | 新增服务器档案（JSON body） |
| `PUT /api/servers/{name}` | 更新服务器档案（密码留空则保留） |
| `DELETE /api/servers/{name}` | 删除服务器档案 |
| `POST /api/test` | 测试连接：`{"server":"档案名"}` 按档案验证，或 `{"url","username","password","no_verify_tls"}` 按自带字段验证；返回 `{"ok":true}` 或 `{"ok":false,"error":"..."}` |
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

# 上传模式（本地目录 -> 远端 WebDAV）：加上 --direction push
webdav-sync --direction push \
  --url https://dav.example.com/dav/backup \
  --local /data/docs \
  --username alice --password "$WEBDAV_PASSWORD" \
  --concurrency 8
```

环境变量等价写法：`WEBDAV_URL`、`WEBDAV_LOCAL_DIR`、`WEBDAV_USERNAME`、`WEBDAV_PASSWORD`、`SYNC_CONCURRENCY`。

### 参数

| 参数 | 说明 |
| --- | --- |
| `--config` | 多任务 JSON 配置（见 `config.example.json`） |
| `--url` / `--local` | 单任务的远端目录 URL 与本地目录 |
| `--direction` | 同步方向：`pull`（默认，下载）/ `push`（上传） |
| `--username` / `--password` | 认证信息（也可用环境变量） |
| `--concurrency` | 并发传输数（默认 8） |
| `--delete` | 清理对端已不存在的文件（pull 删本地 / push 删远端） |
| `--no-verify-tls` | 跳过 TLS 证书校验（自签名证书场景） |
| `--dry-run` | 只打印将要传输的文件 |

## 配置文件（多任务）

```json
{
  "concurrency": 8,
  "delete": false,
  "servers": [
    { "name": "alist-gd", "url": "https://dav.example.com/dav", "username": "alice", "password": "${WEBDAV_PASSWORD}" }
  ],
  "tasks": [
    { "name": "docs",   "server": "alist-gd", "local": "/data/docs",  "concurrency": 8 },
    { "name": "photos", "url": "https://dav.example.com/dav/photos", "local": "/data/photos", "username": "alice", "password": "${WEBDAV_PASSWORD}", "concurrency": 16 }
  ]
}
```

- 顶层 `concurrency` / `delete` 为全局默认，可被单个任务覆盖。
- `servers` 为可复用的 WebDAV 服务器档案；任务用 `server` 字段引用档案名即可复用其连接（URL/账号/密码/TLS），任务自带的非空字段会覆盖档案对应字段。
- 任务可通过 `direction` 指定方向：`pull`（默认，远端 → 本地）或 `push`（本地 → 远端）。`push` 会把 `local` 目录递归上传到 `url` 指向的远端目录，并自动创建远端目录。
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
