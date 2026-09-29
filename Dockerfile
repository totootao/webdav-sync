# 语法: 纯 Go 标准库实现，无第三方依赖
FROM golang:1.21-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY . ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/webdav-sync .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/webdav-sync /usr/local/bin/webdav-sync

# /data 挂载点：存放同步目标目录与配置文件 config.json
VOLUME ["/data"]
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["webdav-sync"]
# 默认启动 Web 管理界面（多任务管理）；可用 `docker run ... -- webdav-sync --url X --local Y` 改为一次性 CLI 同步
CMD ["--web", "--addr", ":8080", "--config", "/data/config.json"]
