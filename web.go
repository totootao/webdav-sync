package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// indexHTML 由 gen_assets.py 从 web/index.html 生成（内联，避免运行时依赖文件）。

// startWeb 启动 Web 管理界面并阻塞。
func startWeb(addr, auth, configPath string, interval int) error {
	store := newStore(configPath)
	if err := store.load(); err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}
	mux := newServer(store)
	var handler http.Handler = mux
	if auth != "" {
		parts := strings.SplitN(auth, ":", 2)
		user, pass := parts[0], ""
		if len(parts) == 2 {
			pass = parts[1]
		}
		handler = basicAuth(user, pass, mux)
	}
	if interval > 0 {
		go store.AutoLoop(interval)
		logf("已启用自动同步，间隔 %d 秒", interval)
	}
	logf("Web 管理界面已启动: http://%s", addr)
	if auth != "" {
		logf("Web 界面已启用 Basic Auth")
	}
	server := &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: 15 * time.Second,
	}
	return server.ListenAndServe()
}

func logf(format string, args ...any) {
	fmt.Printf("[web] "+format+"\n", args...)
}

// newServer 构造路由。
func newServer(store *TaskStore) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write(indexHTML)
	})
	mux.HandleFunc("/api/tasks", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, store.PublicTasks())
		case http.MethodPost:
			var t TaskConfig
			if err := decodeJSON(r, &t); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := store.AddTask(t); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			writeJSON(w, map[string]any{"ok": true, "task": t.Name})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/api/tasks/", func(w http.ResponseWriter, r *http.Request) {
		// /api/tasks/{name} 或 /api/tasks/{name}/run 或 /api/tasks/{name}/logs
		rest := strings.TrimPrefix(r.URL.Path, "/api/tasks/")
		parts := strings.Split(rest, "/")
		name := parts[0]
		if name == "" {
			http.Error(w, "缺少任务名", 400)
			return
		}
		action := ""
		if len(parts) > 1 {
			action = parts[1]
		}
		switch action {
		case "":
			switch r.Method {
			case http.MethodPut:
				var t TaskConfig
				if err := decodeJSON(r, &t); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				if err := store.UpdateTask(name, t); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				writeJSON(w, map[string]any{"ok": true})
			case http.MethodDelete:
				if err := store.DeleteTask(name); err != nil {
					http.Error(w, err.Error(), 400)
					return
				}
				writeJSON(w, map[string]any{"ok": true})
			default:
				http.Error(w, "method not allowed", 405)
			}
		case "run":
			if r.Method != http.MethodPost {
				http.Error(w, "method not allowed", 405)
				return
			}
			if err := store.RunAsync(name); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			writeJSON(w, map[string]any{"ok": true})
		case "logs":
			if r.Method != http.MethodGet {
				http.Error(w, "method not allowed", 405)
				return
			}
			writeJSON(w, map[string]any{"lines": store.Logs(name)})
		default:
			http.NotFound(w, r)
		}
	})
	mux.HandleFunc("/api/servers", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, store.PublicServers())
		case http.MethodPost:
			var sv WebDAVServer
			if err := decodeJSON(r, &sv); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := store.AddServer(sv); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			writeJSON(w, map[string]any{"ok": true, "server": sv.Name})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/api/servers/", func(w http.ResponseWriter, r *http.Request) {
		rest := strings.TrimPrefix(r.URL.Path, "/api/servers/")
		name := strings.Split(rest, "/")[0]
		if name == "" {
			http.Error(w, "缺少服务器名", 400)
			return
		}
		switch r.Method {
		case http.MethodPut:
			var sv WebDAVServer
			if err := decodeJSON(r, &sv); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := store.UpdateServer(name, sv); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			writeJSON(w, map[string]any{"ok": true})
		case http.MethodDelete:
			if err := store.DeleteServer(name); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			writeJSON(w, map[string]any{"ok": true})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/api/test", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var body struct {
			// 来源端
			Server      string `json:"server"`
			URL         string `json:"url"`
			Username    string `json:"username"`
			Password    string `json:"password"`
			NoVerifyTLS bool   `json:"no_verify_tls"`
			// 目标端
			DstServer      string `json:"dst_server"`
			DstURL         string `json:"dst_url"`
			DstUsername    string `json:"dst_username"`
			DstPassword    string `json:"dst_password"`
			DstNoVerifyTLS bool   `json:"dst_no_verify_tls"`
		}
		if err := decodeJSON(r, &body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
	// 解析来源端（若提供了 server 或 url）。
	var srcEp *endpoint
	if body.Server != "" || body.URL != "" {
		ep, err := resolveEndpointFromBody(store, body.Server, body.URL, body.Username, body.Password, body.NoVerifyTLS)
		if err != nil {
			http.Error(w, "来源端: "+err.Error(), 400)
			return
		}
		srcEp = ep
	}
	// 解析目标端（若提供了 dst_server 或 dst_url）。
	var dstEp *endpoint
	if body.DstServer != "" || body.DstURL != "" {
		ep, err := resolveEndpointFromBody(store, body.DstServer, body.DstURL, body.DstUsername, body.DstPassword, body.DstNoVerifyTLS)
		if err != nil {
			http.Error(w, "目标端: "+err.Error(), 400)
			return
		}
		dstEp = ep
	}
		resp := map[string]any{"ok": true}
		if srcEp != nil {
			if err := testConnection(*srcEp); err != nil {
				resp["ok"] = false
				resp["src"] = err.Error()
			} else {
				resp["src"] = "ok"
			}
		}
		if dstEp != nil {
			if err := testConnection(*dstEp); err != nil {
				resp["ok"] = false
				resp["dst"] = err.Error()
			} else {
				resp["dst"] = "ok"
			}
		}
		if srcEp == nil && dstEp == nil {
			http.Error(w, "未提供任何连接信息（server/url 或 dst_server/dst_url）", 400)
			return
		}
		writeJSON(w, resp)
	})
	mux.HandleFunc("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		streamLogs(w, r, store)
	})

	// /api/browse-remote 列举远端 WebDAV 指定目录下的子项，用于前端「浏览目录」选择子目录。
	mux.HandleFunc("/api/browse-remote", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var body struct {
			Server      string `json:"server"`
			URL         string `json:"url"`
			Username    string `json:"username"`
			Password    string `json:"password"`
			NoVerifyTLS bool   `json:"no_verify_tls"`
			Path        string `json:"path"` // 相对根的路径（含前导 /），"" 表示根目录
		}
		if err := decodeJSON(r, &body); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		ep, err := resolveEndpointFromBody(store, body.Server, body.URL, body.Username, body.Password, body.NoVerifyTLS)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		c, err := newClient("(browse)", *ep)
		if err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		rel := strings.TrimRight(body.Path, "/")
		if rel != "" && !strings.HasPrefix(rel, "/") {
			rel = "/" + rel
		}
		entries, err := c.listDir(rel)
		if err != nil {
			http.Error(w, "列举目录失败: "+err.Error(), 400)
			return
		}
		type item struct {
			Name  string `json:"name"`
			Path  string `json:"path"` // 相对根的路径
			IsDir bool   `json:"is_dir"`
			Size  int64  `json:"size"`
		}
		out := make([]item, 0, len(entries))
		for _, e := range entries {
			name := e.path
			if i := strings.LastIndex(strings.TrimRight(name, "/"), "/"); i >= 0 {
				name = name[i+1:]
			}
			if name == "" {
				name = e.path
			}
			out = append(out, item{Name: name, Path: e.path, IsDir: e.isDir, Size: e.size})
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].IsDir != out[j].IsDir {
				return out[i].IsDir // 目录在前
			}
			return out[i].Name < out[j].Name
		})
		writeJSON(w, map[string]any{"ok": true, "base": ep.URL, "path": rel, "entries": out})
	})

	// /api/browse-local 列举本机（运行 webdav-sync 的机器）文件系统中的子目录，用于本地目录选择。
	mux.HandleFunc("/api/browse-local", func(w http.ResponseWriter, r *http.Request) {
		var reqPath string
		if r.Method == http.MethodPost {
			var b struct {
				Path string `json:"path"`
			}
			if err := decodeJSON(r, &b); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			reqPath = b.Path
		} else {
			reqPath = r.URL.Query().Get("path")
		}
		p := filepath.Clean(reqPath)
		if !filepath.IsAbs(p) {
			if reqPath == "" {
				p = "/"
			} else {
				http.Error(w, "路径必须为绝对路径: "+reqPath, 400)
				return
			}
		}
		info, err := os.Stat(p)
		if err != nil || !info.IsDir() {
			http.Error(w, "目录不存在或无法访问: "+p, 400)
			return
		}
		dirEntries, err := os.ReadDir(p)
		if err != nil {
			http.Error(w, "读取目录失败: "+err.Error(), 400)
			return
		}
		type item struct {
			Name  string `json:"name"`
			Path  string `json:"path"`
			IsDir bool   `json:"is_dir"`
			Size  int64  `json:"size"`
		}
		out := make([]item, 0, len(dirEntries))
		for _, e := range dirEntries {
			fi, _ := e.Info()
			var sz int64
			if fi != nil {
				sz = fi.Size()
			}
			full := filepath.Join(p, e.Name())
			out = append(out, item{Name: e.Name(), Path: full, IsDir: e.IsDir(), Size: sz})
		}
		sort.Slice(out, func(i, j int) bool {
			if out[i].IsDir != out[j].IsDir {
				return out[i].IsDir
			}
			return out[i].Name < out[j].Name
		})
		parent := filepath.Dir(p)
		if parent == p {
			parent = "/"
		}
		writeJSON(w, map[string]any{"ok": true, "path": p, "parent": parent, "entries": out})
	})

	return mux
}

// resolveEndpointFromBody 根据表单（server 档案名 或 url/账号/密码）解析出一个 WebDAV 连接端点。
// 优先使用 server 档案（合并其 url/账号/密码/TLS）；否则使用显式 url。
func resolveEndpointFromBody(store *TaskStore, server, url, username, password string, noVerifyTLS bool) (*endpoint, error) {
	if server != "" {
		for _, sv := range store.allServers() {
			if sv.Name == server {
				return &endpoint{URL: sv.URL, Username: sv.Username, Password: sv.Password, NoVerifyTLS: sv.NoVerifyTLS}, nil
			}
		}
		return nil, fmt.Errorf("服务器档案不存在: %s", server)
	}
	if url != "" {
		return &endpoint{URL: url, Username: username, Password: password, NoVerifyTLS: noVerifyTLS}, nil
	}
	return nil, fmt.Errorf("未提供 server 或 url")
}

func basicAuth(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok || u != user || p != pass {
			w.Header().Set("WWW-Authenticate", `Basic realm="webdav-sync"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func streamLogs(w http.ResponseWriter, r *http.Request, store *TaskStore) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming unsupported", 500)
		return
	}
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	ch := make(chan string, 64)
	store.global.Subscribe(ch)
	defer store.global.Unsubscribe(ch)

	// 先回放历史日志
	for _, line := range store.GlobalLogs() {
		fmt.Fprintf(w, "data: %s\n\n", line)
	}
	flusher.Flush()

	ticker := time.NewTicker(25 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case line := <-ch:
			fmt.Fprintf(w, "data: %s\n\n", line)
			flusher.Flush()
		case <-ticker.C:
			// 心跳，保持连接
			fmt.Fprintf(w, ": ping\n\n")
			flusher.Flush()
		}
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	return dec.Decode(v)
}
