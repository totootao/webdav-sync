package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// indexHTML 由 gen_assets.py 从 web/index.html 生成（内联，避免运行时依赖文件）。

// 会话 Cookie 名称。
const sessionCookieName = "wdsess"

// sessionDays 登录后免登录天数（勾选「保持登录」时生效）。
const sessionDays = 90

// startWeb 启动 Web 管理界面并阻塞。
func startWeb(addr, auth, configPath string, interval int) error {
	store := newStore(configPath)
	if err := store.load(); err != nil {
		return fmt.Errorf("加载配置失败: %w", err)
	}
	user, pass := "", ""
	if auth != "" {
		parts := strings.SplitN(auth, ":", 2)
		user = parts[0]
		if len(parts) == 2 {
			pass = parts[1]
		}
	}
	mux := newServer(store, user, pass)
	var handler http.Handler = mux
	if auth != "" {
		// 登录会话（可免登录 90 天）为主，Basic Auth 仅作兼容（脚本/旧书签）。
		handler = authMiddleware(user, pass, mux)
		logf("已启用登录保护（登录一次可免登录 %d 天）", sessionDays)
	} else {
		logf("未配置 --web-auth：Web 界面无需登录即可访问（建议加上 --web-auth 用户名:密码）")
	}
	if interval > 0 {
		go store.AutoLoop(interval)
		logf("已启用自动同步，间隔 %d 秒", interval)
	}
	logf("Web 管理界面已启动: http://%s", addr)
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

// newServer 构造路由。webUser/webPass 为 --web-auth 配置的登录账号（为空表示未启用登录）。
func newServer(store *TaskStore, webUser, webPass string) *http.ServeMux {
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

	// /api/me 返回当前登录状态（前端据此判断是否显示登录框）。
	mux.HandleFunc("/api/me", func(w http.ResponseWriter, r *http.Request) {
		if webUser == "" {
			writeJSON(w, map[string]any{"ok": true, "auth_enabled": false, "days": sessionDays})
			return
		}
		if authenticated(r, webUser, webPass) {
			writeJSON(w, map[string]any{"ok": true, "auth_enabled": true, "user": webUser, "days": sessionDays})
			return
		}
		writeJSONStatus(w, http.StatusUnauthorized, map[string]any{"error": "未登录或登录已过期", "auth_enabled": true})
	})

	// /api/login 校验账号密码并下发会话 Cookie（勾选「保持登录」可免登录 90 天）。
	mux.HandleFunc("/api/login", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", 405)
			return
		}
		var b struct {
			User     string `json:"user"`
			Password string `json:"password"`
			Remember bool   `json:"remember"`
		}
		if err := decodeJSON(r, &b); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		if webUser == "" { // 未启用登录，直接放行
			writeJSON(w, map[string]any{"ok": true, "auth_enabled": false})
			return
		}
		if !constantTimeEqual(b.User, webUser) || !constantTimeEqual(b.Password, webPass) {
			writeJSONStatus(w, http.StatusUnauthorized, map[string]any{"error": "用户名或密码错误"})
			return
		}
		days := 0
		if b.Remember {
			days = sessionDays
		}
		setSessionCookie(w, r, webUser, webPass, days)
		logf("用户登录成功: %s（免登录 %d 天）", webUser, days)
		writeJSON(w, map[string]any{"ok": true, "user": webUser, "days": days})
	})

	// /api/logout 清除会话 Cookie 并使该令牌立即失效。
	mux.HandleFunc("/api/logout", func(w http.ResponseWriter, r *http.Request) {
		if c, err := r.Cookie(sessionCookieName); err == nil {
			revokeSession(c.Value)
		}
		http.SetCookie(w, &http.Cookie{
			Name:     sessionCookieName,
			Value:    "",
			Path:     "/",
			MaxAge:   -1,
			HttpOnly: true,
			SameSite: http.SameSiteLaxMode,
		})
		writeJSON(w, map[string]any{"ok": true})
	})

	return mux
}

// ---------------------------------------------------------------------------
// 登录与会话
// ---------------------------------------------------------------------------

// sessionKey 由账号密码派生签名密钥：进程重启后已签发的 Cookie 仍然有效，
// 修改密码则全部失效。无需额外持久化。
func sessionKey(user, pass string) string {
	sum := sha256.Sum256([]byte("webdav-sync-session-v1|" + user + "|" + pass))
	return hex.EncodeToString(sum[:])
}

// signSession 生成 "过期时间戳.HMAC" 形式的会话令牌。
func signSession(user string, exp int64, key string) string {
	payload := fmt.Sprintf("%s|%d", user, exp)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(payload))
	return fmt.Sprintf("%d.%s", exp, hex.EncodeToString(mac.Sum(nil)))
}

// verifySession 校验令牌是否由本服务签发且未过期。
func verifySession(tok, key, user string) bool {
	parts := strings.SplitN(tok, ".", 2)
	if len(parts) != 2 {
		return false
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || exp <= 0 {
		return false
	}
	if time.Now().Unix() > exp {
		return false
	}
	want := signSession(user, exp, key)
	return hmac.Equal([]byte(tok), []byte(want))
}

// setSessionCookie 下发会话 Cookie；days<=0 表示浏览器会话级 Cookie（关闭即失效）。
func setSessionCookie(w http.ResponseWriter, r *http.Request, user, pass string, days int) {
	exp := time.Now().AddDate(0, 0, days)
	maxAge := 0
	if days > 0 {
		maxAge = days * 24 * 60 * 60
	}
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    signSession(user, exp.Unix(), sessionKey(user, pass)),
		Path:     "/",
		MaxAge:   maxAge,
		Expires:  exp,
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		Secure:   isHTTPS(r),
	})
}

// revokedSessions 记录已登出的令牌（进程内存）。登出后旧 Cookie 立即失效，
// 避免无状态的 HMAC 令牌在过期前仍可继续使用。
var revokedSessions = struct {
	mu sync.Mutex
	m  map[string]int64 // token -> 令牌过期时间戳，便于清理
}{m: map[string]int64{}}

// revokeSession 让指定令牌立即失效，并顺带清理已过期的记录。
func revokeSession(tok string) {
	exp := tokenExpiry(tok)
	revokedSessions.mu.Lock()
	defer revokedSessions.mu.Unlock()
	revokedSessions.m[tok] = exp
	if len(revokedSessions.m) > 512 {
		now := time.Now().Unix()
		for k, e := range revokedSessions.m {
			if e <= now {
				delete(revokedSessions.m, k)
			}
		}
	}
}

func isRevoked(tok string) bool {
	revokedSessions.mu.Lock()
	defer revokedSessions.mu.Unlock()
	_, ok := revokedSessions.m[tok]
	return ok
}

// tokenExpiry 解析令牌中的过期时间戳；解析失败返回 0。
func tokenExpiry(tok string) int64 {
	parts := strings.SplitN(tok, ".", 2)
	if len(parts) != 2 {
		return 0
	}
	exp, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0
	}
	return exp
}

// authenticated 判断是否已登录：会话 Cookie（且未登出）或 Basic Auth（兼容脚本）任一通过即可。
func authenticated(r *http.Request, user, pass string) bool {
	if c, err := r.Cookie(sessionCookieName); err == nil &&
		verifySession(c.Value, sessionKey(user, pass), user) && !isRevoked(c.Value) {
		return true
	}
	if u, pw, ok := r.BasicAuth(); ok && constantTimeEqual(u, user) && constantTimeEqual(pw, pass) {
		return true
	}
	return false
}

// authMiddleware 保护所有接口：登录/登出/自身信息与首页放行，其余需已登录。
func authMiddleware(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		// 放行：登录、登出、登录状态查询、首页（前端自行展示登录框）
		if p == "/api/login" || p == "/api/logout" || p == "/api/me" || p == "/" {
			next.ServeHTTP(w, r)
			return
		}
		if authenticated(r, user, pass) {
			next.ServeHTTP(w, r)
			return
		}
		if strings.HasPrefix(p, "/api/") {
			writeJSONStatus(w, http.StatusUnauthorized, map[string]any{"error": "未登录或登录已过期"})
			return
		}
		http.Redirect(w, r, "/", http.StatusFound)
	})
}

func isHTTPS(r *http.Request) bool {
	if r.TLS != nil {
		return true
	}
	return strings.EqualFold(r.Header.Get("X-Forwarded-Proto"), "https")
}

// constantTimeEqual 常量时间字符串比较，避免时序侧信道。
func constantTimeEqual(a, b string) bool {
	return hmac.Equal([]byte(a), []byte(b))
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

// writeJSONStatus 以指定状态码输出 JSON。
func writeJSONStatus(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	defer r.Body.Close()
	dec := json.NewDecoder(r.Body)
	return dec.Decode(v)
}
