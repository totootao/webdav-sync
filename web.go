package main

import (
	"encoding/json"
	"fmt"
	"net/http"
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
	mux.HandleFunc("/api/config", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			writeJSON(w, store.PublicConfig())
		case http.MethodPut:
			var c Config
			if err := decodeJSON(r, &c); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			if err := store.UpdateConfig(c); err != nil {
				http.Error(w, err.Error(), 400)
				return
			}
			writeJSON(w, map[string]any{"ok": true})
		default:
			http.Error(w, "method not allowed", 405)
		}
	})
	mux.HandleFunc("/api/stream", func(w http.ResponseWriter, r *http.Request) {
		streamLogs(w, r, store)
	})
	return mux
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
