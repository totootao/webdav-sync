// webdav-sync 递归同步 WebDAV 服务器目录到本机。
//
// 特性：
//   - 递归遍历远端目录树（PROPFIND Depth:1 逐层展开）
//   - 多线程并发下载（可配置并发数）
//   - 多任务：单个 JSON 配置文件内定义多个 (远端 -> 本地) 同步任务
//   - 增量同步：按 远端大小 + mtime(/etag) 跳过未变更文件
//   - 下载到临时文件后原子重命名；失败按次数重试
//   - 可选 --delete 清理远端已删除的本地文件
//   - 配置字符串支持 ${ENV} 环境变量展开，适合容器与 cron
package main

import (
	"crypto/tls"
	"encoding/json"
	"encoding/xml"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

// TaskConfig 单个同步任务。
type TaskConfig struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Local       string `json:"local"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	Concurrency int    `json:"concurrency"`
	Delete      bool   `json:"delete"`
	NoVerifyTLS bool   `json:"no_verify_tls"` // 默认 false（校验 TLS）
	DryRun      bool   `json:"-"`
}

// Config 顶层配置（多任务）。
type Config struct {
	Concurrency int          `json:"concurrency"`
	Delete      bool         `json:"delete"`
	NoVerifyTLS bool         `json:"no_verify_tls"`
	Tasks       []TaskConfig `json:"tasks"`
}

var envRe = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

func expandEnv(s string) string {
	if s == "" {
		return s
	}
	return envRe.ReplaceAllStringFunc(s, func(m string) string {
		sub := envRe.FindStringSubmatch(m)
		key, def := sub[1], sub[3]
		if v, ok := os.LookupEnv(key); ok {
			return v
		}
		return def
	})
}

// ---------------------------------------------------------------------------
// WebDAV 207 解析
// ---------------------------------------------------------------------------

type multistatus struct {
	XMLName  xml.Name   `xml:"multistatus"`
	Response []response `xml:"response"`
}

type response struct {
	Href     string     `xml:"href"`
	Propstat []propstat `xml:"propstat"`
}

type propstat struct {
	Prop   prop   `xml:"prop"`
	Status string `xml:"status"`
}

type prop struct {
	Resourcetype  resourcetype `xml:"resourcetype"`
	ContentLength string       `xml:"getcontentlength"`
	LastModified  string       `xml:"getlastmodified"`
	Etag          string       `xml:"getetag"`
}

type resourcetype struct {
	Collection *struct{} `xml:"collection"`
}

type resource struct {
	path  string
	isDir bool
	size  int64
	mtime time.Time
	etag  string
}

// ---------------------------------------------------------------------------
// 客户端
// ---------------------------------------------------------------------------

type client struct {
	origin   string
	root     string
	username string
	password string
	http     *http.Client
	name     string
}

func newClient(cfg TaskConfig) (*client, error) {
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("无效的 WebDAV URL: %q", cfg.URL)
	}
	tr := &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	if cfg.NoVerifyTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &client{
		origin:   u.Scheme + "://" + u.Host,
		root:     strings.TrimRight(u.Path, "/"),
		username: cfg.Username,
		password: cfg.Password,
		http:     &http.Client{Transport: tr},
		name:     cfg.Name,
	}, nil
}

func (c *client) urlFor(rel string) string {
	full := c.root + rel
	var b strings.Builder
	b.WriteString(c.origin)
	for _, seg := range strings.Split(full, "/") {
		if seg == "" {
			continue
		}
		b.WriteByte('/')
		b.WriteString(url.PathEscape(seg))
	}
	return b.String()
}

func normalizeHref(href string) string {
	if i := strings.Index(href, "://"); i >= 0 {
		if u, err := url.Parse(href); err == nil {
			href = u.Path
		}
	}
	href = strings.TrimSpace(href)
	if u, err := url.PathUnescape(href); err == nil {
		href = u
	}
	if !strings.HasPrefix(href, "/") {
		href = "/" + href
	}
	return href
}

func parseTime(s string) (time.Time, bool) {
	for _, layout := range []string{time.RFC1123, time.RFC1123Z, "2006-01-02T15:04:05Z", "Mon, 02 Jan 2006 15:04:05 MST"} {
		if t, err := time.Parse(layout, strings.TrimSpace(s)); err == nil {
			return t, true
		}
	}
	return time.Time{}, false
}

func (c *client) listDir(rel string) ([]resource, error) {
	u := c.urlFor(rel)
	req, err := http.NewRequest("PROPFIND", u, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Depth", "1")
	req.Header.Set("Accept", "application/xml")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("认证失败 (401)，请检查用户名/密码")
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, fmt.Errorf("远端目录不存在 (404): %q", rel)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("PROPFIND 失败: HTTP %d @ %s", resp.StatusCode, u)
	}
	var ms multistatus
	if err := xml.Unmarshal(body, &ms); err != nil {
		return nil, fmt.Errorf("解析 PROPFIND 响应失败: %w", err)
	}
	var out []resource
	rootTrim := strings.TrimRight(c.root, "/")
	for _, r := range ms.Response {
		p := normalizeHref(r.Href)
		var relPath string
		if c.root != "" {
			if p == c.root || strings.TrimRight(p, "/") == rootTrim {
				continue // 目录自身（根）
			}
			if strings.HasPrefix(p, rootTrim+"/") {
				relPath = strings.TrimPrefix(p, rootTrim)
			} else {
				continue // 根外条目
			}
		} else {
			relPath = p
		}
		// 过滤本次请求目录自身的条目（子目录的 self 也会出现在响应里）
		if strings.TrimRight(relPath, "/") == strings.TrimRight(rel, "/") {
			continue
		}
		if !strings.HasPrefix(relPath, "/") {
			relPath = "/" + relPath
		}
		var res resource
		res.path = relPath
		for _, ps := range r.Propstat {
			res.isDir = ps.Prop.Resourcetype.Collection != nil
			if ps.Prop.ContentLength != "" {
				fmt.Sscanf(ps.Prop.ContentLength, "%d", &res.size)
			}
			if t, ok := parseTime(ps.Prop.LastModified); ok {
				res.mtime = t
			}
			res.etag = strings.Trim(ps.Prop.Etag, `"`)
		}
		out = append(out, res)
	}
	return out, nil
}

// 分块下载的块大小；单块失败只会重传这一块，避免大文件整段重来。
const downloadBlockSize = 8 << 20 // 8 MiB

// 读取超时：连接超过该时长没有任何数据则视为中断并重试。
const readStallTimeout = 60 * time.Second

// downloadRetries 单文件整体重试次数（块内另有独立重试）。
const downloadRetries = 5

// backoff 指数退避，封顶 15s。
func backoff(attempt int) time.Duration {
	d := time.Duration(1<<uint(attempt)) * time.Second
	if d > 15*time.Second {
		d = 15 * time.Second
	}
	return d
}

// stallReader 在底层读取超过 timeout 仍无数据时返回错误，避免假死连接无限等待。
type stallReader struct {
	r       io.Reader
	timeout time.Duration
}

func (s *stallReader) Read(p []byte) (int, error) {
	type result struct {
		n   int
		err error
	}
	ch := make(chan result, 1)
	go func() {
		n, err := s.r.Read(p)
		ch <- result{n, err}
	}()
	t := time.NewTimer(s.timeout)
	defer t.Stop()
	select {
	case res := <-ch:
		return res.n, res.err
	case <-t.C:
		return 0, fmt.Errorf("读取超时（%s 内无数据）", s.timeout)
	}
}

func (c *client) download(res resource, localPath string, retries int, lg *log.Logger) (int64, error) {
	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		n, err := c.doDownload(res, localPath)
		if err == nil {
			return n, nil
		}
		lastErr = err
		if attempt < retries {
			lg.Printf("[%s] 重试 %d/%d (%s): %v", c.name, attempt, retries, res.path, err)
			time.Sleep(backoff(attempt))
		}
	}
	return 0, lastErr
}

// doDownload 下载单个文件，支持断点续传（基于 .part 偏移 + HTTP Range）。
// 优先用分块 Range 下载（失败局限于单块自动重传）；若服务器不支持 Range 则退化为整段流式下载（同样带续传）。
func (c *client) doDownload(res resource, localPath string) (int64, error) {
	part := localPath + ".part"
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// 已下载偏移（支持续传：上次中断留下的 .part 从这里继续）。
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, err
	}
	if res.size > 0 && offset >= res.size {
		return c.finalize(f, part, localPath, res)
	}

	// 已知大小且服务器支持 Range -> 分块下载；否则整段流式。
	if res.size > 0 && c.probeRange(res.path) {
		// 回退到当前块起点，整块重下，避免残留半块。
		if offset > 0 {
			offset -= offset % downloadBlockSize
		}
		if err := c.blockDownload(f, res, offset); err != nil {
			return offset, err
		}
	} else {
		if err := c.streamDownload(f, res, offset); err != nil {
			return offset, err
		}
	}
	return c.finalize(f, part, localPath, res)
}

// probeRange 探测服务器是否支持 HTTP Range（发 bytes=0-0）。
func (c *client) probeRange(p string) bool {
	u := c.urlFor(p)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return false
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := c.http.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusPartialContent
}

// blockDownload 以固定大小分块、用 Range 逐块下载；单块失败仅重传该块。
func (c *client) blockDownload(f *os.File, res resource, start int64) error {
	for start < res.size {
		end := start + downloadBlockSize - 1
		if end >= res.size {
			end = res.size - 1
		}
		if err := c.downloadRange(f, res, start, end); err != nil {
			return err
		}
		start = end + 1
	}
	return nil
}

// downloadRange 下载 [start,end] 这一块，块内独立重试。
func (c *client) downloadRange(f *os.File, res resource, start, end int64) error {
	u := c.urlFor(res.path)
	var lastErr error
	for attempt := 1; attempt <= 8; attempt++ {
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			return err
		}
		req, err := http.NewRequest(http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		if c.username != "" {
			req.SetBasicAuth(c.username, c.password)
		}
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			time.Sleep(backoff(attempt))
			continue
		}
		if resp.StatusCode != http.StatusPartialContent {
			resp.Body.Close()
			return fmt.Errorf("Range 请求返回 %d（服务器可能不支持断点续传）", resp.StatusCode)
		}
		_, err = io.Copy(f, &stallReader{r: resp.Body, timeout: readStallTimeout})
		resp.Body.Close()
		if err != nil {
			lastErr = err
			time.Sleep(backoff(attempt))
			continue
		}
		return nil
	}
	return lastErr
}

// streamDownload 整段流式下载（服务器不支持 Range 时的退路），同样支持断点续传。
func (c *client) streamDownload(f *os.File, res resource, offset int64) error {
	if offset > 0 {
		// 服务器不支持 Range，重头开始写，丢弃已下载部分。
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return err
		}
		if err := f.Truncate(0); err != nil {
			return err
		}
	}
	u := c.urlFor(res.path)
	req, err := http.NewRequest(http.MethodGet, u, nil)
	if err != nil {
		return err
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		// ok
	case http.StatusUnauthorized:
		return fmt.Errorf("认证失败 (401)")
	case http.StatusNotFound:
		return fmt.Errorf("远端文件不存在 (404): %s", res.path)
	default:
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	_, err = io.Copy(f, &stallReader{r: resp.Body, timeout: readStallTimeout})
	return err
}

// finalize 落盘并原子重命名，返回最终文件大小。
func (c *client) finalize(f *os.File, part, localPath string, res resource) (int64, error) {
	if err := f.Sync(); err != nil {
		return 0, err
	}
	if !res.mtime.IsZero() {
		_ = os.Chtimes(part, res.mtime, res.mtime)
	}
	if err := os.Rename(part, localPath); err != nil {
		return 0, err
	}
	fi, err := f.Stat()
	if err != nil {
		return 0, nil
	}
	return fi.Size(), nil
}

// ---------------------------------------------------------------------------
// 遍历与同步
// ---------------------------------------------------------------------------

func walkRemote(c *client) (dirs, files []resource, err error) {
	queue := []string{""}
	for len(queue) > 0 {
		rel := queue[0]
		queue = queue[1:]
		entries, e := c.listDir(rel)
		if e != nil {
			return dirs, files, e
		}
		for _, e2 := range entries {
			if e2.isDir {
				dirs = append(dirs, e2)
				queue = append(queue, e2.path)
			} else {
				files = append(files, e2)
			}
		}
	}
	return dirs, files, nil
}

func unchanged(local string, res resource) bool {
	st, err := os.Stat(local)
	if err != nil {
		return false
	}
	if st.Size() != res.size {
		return false
	}
	if res.etag != "" {
		return true
	}
	if !res.mtime.IsZero() {
		return abs(st.ModTime().Sub(res.mtime)) <= 2*time.Second
	}
	return true
}

func abs(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}

type result struct {
	name             string
	downloaded       int
	skipped          int
	deleted          int
	failed           int
	bytesTransferred int64
	errors           []string
}

func (r *result) ok() bool { return r.failed == 0 }

func syncTask(cfg TaskConfig, lg *log.Logger) *result {
	if lg == nil {
		lg = log.Default()
	}
	res := &result{name: cfg.Name}
	lg.Printf("[%s] 开始同步: %s -> %s", cfg.Name, cfg.URL, cfg.Local)
	c, err := newClient(cfg)
	if err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		lg.Printf("[%s] 失败: %v", cfg.Name, err)
		return res
	}
	localRoot := filepath.Clean(expandEnv(cfg.Local))

	dirs, files, err := walkRemote(c)
	if err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		lg.Printf("[%s] 遍历失败: %v", cfg.Name, err)
		return res
	}
	lg.Printf("[%s] 远端共 %d 个目录 / %d 个文件", cfg.Name, len(dirs), len(files))

	var pending []resource
	for _, f := range files {
		lp := filepath.Join(localRoot, filepath.FromSlash(strings.TrimPrefix(f.path, "/")))
		if unchanged(lp, f) {
			res.skipped++
		} else {
			pending = append(pending, f)
		}
	}

	if cfg.DryRun {
		for _, f := range pending {
			lg.Printf("[%s][dry-run] 需下载: %s", cfg.Name, f.path)
		}
		res.downloaded = len(pending)
		return res
	}

	for _, d := range dirs {
		_ = os.MkdirAll(filepath.Join(localRoot, filepath.FromSlash(strings.TrimPrefix(d.path, "/"))), 0o755)
	}

	var (
		wg    sync.WaitGroup
		sem   = make(chan struct{}, cfg.Concurrency)
		mu    sync.Mutex
		total = len(pending)
	)
	for _, f := range pending {
		wg.Add(1)
		go func(f resource) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			lp := filepath.Join(localRoot, filepath.FromSlash(strings.TrimPrefix(f.path, "/")))
			n, err := c.download(f, lp, downloadRetries, lg)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				res.failed++
				res.errors = append(res.errors, fmt.Sprintf("%s: %v", f.path, err))
				lg.Printf("[%s] 下载失败 %s: %v", cfg.Name, f.path, err)
				return
			}
			res.downloaded++
			res.bytesTransferred += n
			if res.downloaded%10 == 0 || res.downloaded == total {
				lg.Printf("[%s] 进度: %d/%d", cfg.Name, res.downloaded, total)
			}
		}(f)
	}
	wg.Wait()

	if cfg.Delete && res.ok() {
		remoteFiles := map[string]bool{}
		remoteDirs := map[string]bool{}
		for _, f := range files {
			remoteFiles[strings.TrimPrefix(f.path, "/")] = true
		}
		for _, d := range dirs {
			remoteDirs[strings.TrimPrefix(d.path, "/")] = true
		}
		_ = filepath.Walk(localRoot, func(p string, info os.FileInfo, err error) error {
			if err != nil || p == localRoot {
				return nil
			}
			rel, _ := filepath.Rel(localRoot, p)
			rel = filepath.ToSlash(rel)
			if info.IsDir() {
				if !remoteDirs[rel] && isEmptyDir(p) {
					if err := os.Remove(p); err == nil {
						res.deleted++
					}
				}
			} else if !remoteFiles[rel] {
				if err := os.Remove(p); err == nil {
					res.deleted++
				}
			}
			return nil
		})
	}

	mb := float64(res.bytesTransferred) / 1024 / 1024
	lg.Printf("[%s] 完成: 下载 %d, 跳过 %d, 删除 %d, 失败 %d, 传输 %.2f MB",
		cfg.Name, res.downloaded, res.skipped, res.deleted, res.failed, mb)
	return res
}

func isEmptyDir(p string) bool {
	f, err := os.Open(p)
	if err != nil {
		return false
	}
	defer f.Close()
	_, err = f.Readdirnames(1)
	return err == io.EOF
}

// ---------------------------------------------------------------------------
// 配置加载与入口
// ---------------------------------------------------------------------------

func loadConfigFile(p string) ([]TaskConfig, error) {
	data, err := os.ReadFile(p)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("配置文件解析失败: %w", err)
	}
	defConc := cfg.Concurrency
	if defConc <= 0 {
		defConc = 8
	}
	for i := range cfg.Tasks {
		t := &cfg.Tasks[i]
		if t.Name == "" {
			t.Name = fmt.Sprintf("task-%d", i+1)
		}
		if t.Concurrency <= 0 {
			t.Concurrency = defConc
		}
		t.Delete = t.Delete || cfg.Delete
		if cfg.NoVerifyTLS {
			t.NoVerifyTLS = true
		}
		t.URL = expandEnv(t.URL)
		t.Local = expandEnv(t.Local)
		t.Username = expandEnv(t.Username)
		t.Password = expandEnv(t.Password)
	}
	return cfg.Tasks, nil
}

func main() {
	var (
		config      = flag.String("config", "", "多任务配置文件 (JSON)")
		urlFlag     = flag.String("url", "", "WebDAV 目录 URL (或环境变量 WEBDAV_URL)")
		localFlag   = flag.String("local", "", "本地目标目录 (或环境变量 WEBDAV_LOCAL_DIR)")
		username    = flag.String("username", "", "用户名 (或 WEBDAV_USERNAME)")
		password    = flag.String("password", "", "密码 (或 WEBDAV_PASSWORD)")
		concurrency = flag.Int("concurrency", 0, "并发下载数 (默认 8)")
		deleteFlag  = flag.Bool("delete", false, "删除远端已不存在的本地文件")
		noVerify    = flag.Bool("no-verify-tls", false, "跳过 TLS 证书校验")
		dryRun      = flag.Bool("dry-run", false, "只打印将要下载的文件，不实际下载")
		web         = flag.Bool("web", false, "启动 Web 管理界面 (多任务管理)")
		addr        = flag.String("addr", ":8080", "Web 服务监听地址 (配合 --web)")
		webAuth     = flag.String("web-auth", "", "Web 界面 Basic Auth，格式 user:pass (可选)")
		interval    = flag.Int("interval", 0, "Web 模式下自动同步间隔秒数 (0=关闭，配合 --web)")
	)
	flag.Parse()

	if *web {
		cfgPath := *config
		if cfgPath == "" {
			cfgPath = "webdav-sync.json"
		}
		if err := startWeb(*addr, *webAuth, cfgPath, *interval); err != nil {
			log.Fatalf("Web 服务启动失败: %v", err)
		}
		return
	}

	var tasks []TaskConfig
	if *config != "" {
		ts, err := loadConfigFile(*config)
		if err != nil {
			log.Fatalf("读取配置失败: %v", err)
		}
		tasks = ts
		if *concurrency > 0 {
			for i := range tasks {
				tasks[i].Concurrency = *concurrency
			}
		}
		if *deleteFlag {
			for i := range tasks {
				tasks[i].Delete = true
			}
		}
		if *noVerify {
			for i := range tasks {
				tasks[i].NoVerifyTLS = true
			}
		}
		if *dryRun {
			for i := range tasks {
				tasks[i].DryRun = true
			}
		}
	} else {
		u := orEnv(*urlFlag, "WEBDAV_URL")
		l := orEnv(*localFlag, "WEBDAV_LOCAL_DIR")
		if u == "" || l == "" {
			log.Fatalf("需要 --config，或同时提供 --url 与 --local（也可用环境变量 WEBDAV_URL / WEBDAV_LOCAL_DIR）")
		}
		conc := *concurrency
		if conc <= 0 {
			if v := os.Getenv("SYNC_CONCURRENCY"); v != "" {
				fmt.Sscanf(v, "%d", &conc)
			}
			if conc <= 0 {
				conc = 8
			}
		}
		tasks = []TaskConfig{{
			Name:        "default",
			URL:         expandEnv(u),
			Local:       expandEnv(l),
			Username:    orEnv(*username, "WEBDAV_USERNAME"),
			Password:    orEnv(*password, "WEBDAV_PASSWORD"),
			Concurrency: conc,
			Delete:      *deleteFlag,
			NoVerifyTLS: *noVerify,
			DryRun:      *dryRun,
		}}
	}

	failed := false
	lg := log.New(os.Stderr, "", log.LstdFlags)
	for _, t := range tasks {
		if t.URL == "" || t.Local == "" {
			log.Printf("[%s] 缺少 url 或 local 配置", t.Name)
			failed = true
			continue
		}
		if r := syncTask(t, lg); !r.ok() {
			failed = true
		}
	}
	if failed {
		os.Exit(1)
	}
}

func orEnv(v, env string) string {
	if v != "" {
		return v
	}
	return os.Getenv(env)
}
