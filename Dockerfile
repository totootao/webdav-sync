# 语法: 纯 Go 标准库实现，无第三方依赖
FROM golang:1.21-alpine AS build
WORKDIR /src
COPY go.mod ./
COPY main.go ./
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /out/webdav-sync .

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata
COPY --from=build /out/webdav-sync /usr/local/bin/webdav-sync

# 默认同步目标目录（可用 -v 挂载）
VOLUME ["/data"]
ENV PYTHONUNBUFFERED=1

ENTRYPOINT ["webdav-sync"]
CMD ["--help"]
