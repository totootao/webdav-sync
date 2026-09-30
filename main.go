// webdav-sync 在本地与 WebDAV 服务器之间递归双向同步。
//
// 同步方向（direction，默认 pull）：
//   - pull：远端 WebDAV 目录 -> 本地（下载）
//   - push：本地目录 -> 远端 WebDAV（上传）
//
// 特性：
//   - 递归遍历目录树（PROPFIND Depth:1 逐层展开 / 本地 filepath.WalkDir）
//   - 多线程并发（可配置并发数）
//   - 多任务：单个 JSON 配置文件内定义多个双向同步任务
//   - 增量同步：按 大小(+mtime/etag) 跳过未变更文件
//   - 下载支持分块 Range 断点续传；上传流式 PUT 失败整段重试
//   - 可选 --delete 清理对端已不存在的文件
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
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 配置
// ---------------------------------------------------------------------------

// WebDAVServer 独立配置的 WebDAV 服务器档案（可复用、可「测试连接」）。
// 同步任务既可直接引用某个档案（TaskConfig.Server），也可自带 url/账号（二选一/互补）。
type WebDAVServer struct {
	Name        string `json:"name"`
	URL         string `json:"url"`
	Username    string `json:"username"`
	Password    string `json:"password"`
	NoVerifyTLS bool   `json:"no_verify_tls"` // 默认 false（校验 TLS）
}

// TaskConfig 单个同步任务。
type TaskConfig struct {
	Name        string `json:"name"`
	Server      string `json:"server"`     // 引用 WebDAVServer 档案名（可选，作为来源端，与自带 url/账号 二选一）
	URL         string `json:"url"`        // 来源端 WebDAV URL（pull / copy 使用）
	URLPath     string `json:"url_path"`  // 来源端：引用档案时的相对子目录（可选，拼接到档案 URL 之后）
	Local       string `json:"local"`      // pull=本地目标 / push=本地来源 / copy=本地中转(可留空)
	Username    string `json:"username"`
	Password    string `json:"password"`
	Direction   string `json:"direction"`  // pull=下载(远端->本地) / push=上传(本地->远端) / copy=互传(远端->远端)
	Concurrency int    `json:"concurrency"`
	Delete      bool   `json:"delete"`
	NoVerifyTLS bool   `json:"no_verify_tls"` // 默认 false（校验 TLS）

	// 目标端 WebDAV（push=上传目标 / copy=互传目标）。可引用档案(dst_server)或自带字段。
	DstServer      string `json:"dst_server"`
	DstURL         string `json:"dst_url"`
	DstURLPath     string `json:"dst_url_path"` // 目标端：引用档案时的相对子目录（可选）
	DstUsername    string `json:"dst_username"`
	DstPassword    string `json:"dst_password"`
	DstNoVerifyTLS bool   `json:"dst_no_verify_tls"`
	DryRun         bool   `json:"-"`
}

// Config 顶层配置（多任务 + 多服务器档案）。
type Config struct {
	Concurrency int           `json:"concurrency"`
	Delete      bool          `json:"delete"`
	NoVerifyTLS bool          `json:"no_verify_tls"`
	Servers     []WebDAVServer `json:"servers"`
	Tasks       []TaskConfig  `json:"tasks"`
}

// mergeServer 将任务引用的服务器档案合并进任务配置：档案提供 url/账号/密码/TLS 默认值，
// 任务自身的非空字段覆盖档案（即「引用档案」与「自带字段」可二选一/互补）。
func mergeServer(cfg TaskConfig, servers []WebDAVServer) TaskConfig {
	if cfg.Server == "" {
		return cfg
	}
	for _, sv := range servers {
		if sv.Name == cfg.Server {
			out := cfg
			if out.URL == "" {
				out.URL = sv.URL
			}
			if out.Username == "" {
				out.Username = sv.Username
			}
		if out.Password == "" {
			out.Password = sv.Password
		}
		out.NoVerifyTLS = out.NoVerifyTLS || sv.NoVerifyTLS
		if out.URLPath != "" {
			out.URL = strings.TrimRight(out.URL, "/") + "/" + strings.TrimLeft(out.URLPath, "/")
		}
		return out
	}
}
return cfg
}

// mergeDstServer 同 mergeServer，但作用于目标端（dst_server / dst_* 字段）。
func mergeDstServer(cfg TaskConfig, servers []WebDAVServer) TaskConfig {
	if cfg.DstServer == "" {
		return cfg
	}
	for _, sv := range servers {
		if sv.Name == cfg.DstServer {
			out := cfg
			if out.DstURL == "" {
				out.DstURL = sv.URL
			}
			if out.DstUsername == "" {
				out.DstUsername = sv.Username
			}
		if out.DstPassword == "" {
			out.DstPassword = sv.Password
		}
		out.DstNoVerifyTLS = out.DstNoVerifyTLS || sv.NoVerifyTLS
		if out.DstURLPath != "" {
			out.DstURL = strings.TrimRight(out.DstURL, "/") + "/" + strings.TrimLeft(out.DstURLPath, "/")
		}
		return out
	}
}
return cfg
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
	dirMu    sync.Mutex
	dirs     map[string]bool // 已确保存在的远端目录缓存（并发上传时复用）
}

// endpoint 表示一个 WebDAV 连接端点（来源或目标）。
type endpoint struct {
	URL         string
	Username    string
	Password    string
	NoVerifyTLS bool
}

func newClient(name string, ep endpoint) (*client, error) {
	u, err := url.Parse(ep.URL)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("无效的 WebDAV URL: %q", ep.URL)
	}
	tr := &http.Transport{
		ResponseHeaderTimeout: 30 * time.Second,
		DialContext: (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
	}
	if ep.NoVerifyTLS {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	return &client{
		origin:   u.Scheme + "://" + u.Host,
		root:     strings.TrimRight(u.Path, "/"),
		username: ep.Username,
		password: ep.Password,
		http:     &http.Client{Transport: tr},
		name:     name,
		dirs:     map[string]bool{},
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

// testConnection 探测 WebDAV 连接是否可用：建立客户端并对根目录做一次 PROPFIND。
// 返回 nil 表示连接/认证正常；否则返回具体错误（网络不通、401 认证失败、404 等）。
func testConnection(ep endpoint) error {
	c, err := newClient("(test)", ep)
	if err != nil {
		return err
	}
	if _, err := c.listDir(""); err != nil {
		return err
	}
	return nil
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
	// 确保目标父目录存在（并发/中转场景下消除 rename 的 ENOENT 竞态）。
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return 0, err
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

// statRemote 对单个远端路径做 PROPFIND(Depth:0)，返回是否存在及其大小/类型。
func (c *client) statRemote(rel string) (*resource, bool, error) {
	u := c.urlFor(rel)
	req, err := http.NewRequest("PROPFIND", u, nil)
	if err != nil {
		return nil, false, err
	}
	req.Header.Set("Depth", "0")
	req.Header.Set("Accept", "application/xml")
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, false, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode == http.StatusNotFound {
		return nil, false, nil
	}
	if resp.StatusCode == http.StatusUnauthorized {
		return nil, false, fmt.Errorf("认证失败 (401)")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, false, fmt.Errorf("PROPFIND 失败: HTTP %d @ %s", resp.StatusCode, rel)
	}
	var ms multistatus
	if err := xml.Unmarshal(body, &ms); err != nil {
		return nil, false, fmt.Errorf("解析 PROPFIND 响应失败: %w", err)
	}
	rootTrim := strings.TrimRight(c.root, "/")
	for _, r := range ms.Response {
		p := normalizeHref(r.Href)
		var relPath string
		if c.root != "" {
			if strings.HasPrefix(p, rootTrim+"/") {
				relPath = strings.TrimPrefix(p, rootTrim)
			} else if p == c.root || strings.TrimRight(p, "/") == rootTrim {
				relPath = ""
			} else {
				continue
			}
		} else {
			relPath = p
		}
		if !strings.HasPrefix(relPath, "/") {
			relPath = "/" + relPath
		}
		// 只取与请求路径精确匹配的那个条目（Depth:0 通常只返回自身）。
		if relPath != rel && strings.TrimRight(relPath, "/") != strings.TrimRight(rel, "/") {
			continue
		}
		res := &resource{path: rel}
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
		return res, true, nil
	}
	return nil, false, nil
}

// mkcol 在远端创建单个集合（目录）；对已存在或父级缺失返回成功（幂等）。
func (c *client) mkcol(rel string) error {
	u := c.urlFor(rel)
	req, err := http.NewRequest("MKCOL", u, nil)
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
	io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK, http.StatusNoContent,
		http.StatusMethodNotAllowed, http.StatusConflict:
		// 405=已存在；409=父级缺失/已存在，按幂等处理。
		return nil
	default:
		return fmt.Errorf("MKCOL 失败: HTTP %d @ %s", resp.StatusCode, rel)
	}
}

// ensureDir 自顶向下确保远端某个目录（及其祖先）存在，结果缓存以并发复用。
func (c *client) ensureDir(rel string) error {
	if rel == "" || rel == "/" {
		return nil
	}
	parts := strings.Split(strings.Trim(rel, "/"), "/")
	cur := ""
	for _, p := range parts {
		cur = cur + "/" + p
		c.dirMu.Lock()
		if c.dirs[cur] {
			c.dirMu.Unlock()
			continue
		}
		c.dirMu.Unlock()
		if err := c.mkcol(cur); err != nil {
			return err
		}
		c.dirMu.Lock()
		c.dirs[cur] = true
		c.dirMu.Unlock()
	}
	return nil
}

// deleteRemote 删除远端单个资源（文件或目录）。
func (c *client) deleteRemote(rel string) error {
	u := c.urlFor(rel)
	req, err := http.NewRequest(http.MethodDelete, u, nil)
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
	io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent, http.StatusNotFound:
		return nil
	default:
		return fmt.Errorf("DELETE 失败: HTTP %d @ %s", resp.StatusCode, rel)
	}
}

// upload 上传单个本地文件到远端，失败按次数整段重试。
func (c *client) upload(localPath, rel string, retries int, lg *log.Logger) (int64, error) {
	var lastErr error
	for attempt := 1; attempt <= retries; attempt++ {
		n, err := c.doUpload(localPath, rel)
		if err == nil {
			return n, nil
		}
		lastErr = err
		if attempt < retries {
			lg.Printf("[%s] 上传重试 %d/%d (%s): %v", c.name, attempt, retries, rel, err)
			time.Sleep(backoff(attempt))
		}
	}
	return 0, lastErr
}

// doUpload 流式 PUT 一个本地文件（设置 Content-Length，带读取超时避免假死）。
func (c *client) doUpload(localPath, rel string) (int64, error) {
	f, err := os.Open(localPath)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	u := c.urlFor(rel)
	req, err := http.NewRequest(http.MethodPut, u, &stallReader{r: f, timeout: readStallTimeout})
	if err != nil {
		return 0, err
	}
	if c.username != "" {
		req.SetBasicAuth(c.username, c.password)
	}
	req.ContentLength = size
	resp, err := c.http.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)
	switch resp.StatusCode {
	case http.StatusCreated, http.StatusOK, http.StatusNoContent:
		return size, nil
	case http.StatusUnauthorized:
		return 0, fmt.Errorf("认证失败 (401)")
	default:
		return 0, fmt.Errorf("PUT 失败: HTTP %d @ %s", resp.StatusCode, rel)
	}
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
	uploaded         int
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
	c, err := newClient(cfg.Name, endpoint{URL: cfg.URL, Username: cfg.Username, Password: cfg.Password, NoVerifyTLS: cfg.NoVerifyTLS})
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

// pushTask 将本地目录递归上传到远端 WebDAV（local -> remote）。
func pushTask(cfg TaskConfig, lg *log.Logger) *result {
	if lg == nil {
		lg = log.Default()
	}
	res := &result{name: cfg.Name}
	lg.Printf("[%s] 开始上传（本地 -> WebDAV）: %s -> %s", cfg.Name, cfg.Local, cfg.URL)
	c, err := newClient(cfg.Name, endpoint{URL: cfg.URL, Username: cfg.Username, Password: cfg.Password, NoVerifyTLS: cfg.NoVerifyTLS})
	if err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		lg.Printf("[%s] 失败: %v", cfg.Name, err)
		return res
	}
	localRoot := filepath.Clean(expandEnv(cfg.Local))
	info, err := os.Stat(localRoot)
	if err != nil || !info.IsDir() {
		res.failed++
		res.errors = append(res.errors, fmt.Sprintf("本地目录不存在或无访问权限: %s", localRoot))
		lg.Printf("[%s] 失败: 本地目录不存在: %s", cfg.Name, localRoot)
		return res
	}

	type localItem struct {
		rel  string
		size int64
	}
	var localFiles []localItem
	localFileSet := map[string]bool{}
	var localDirs []string
	if err := filepath.WalkDir(localRoot, func(p string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(localRoot, p)
		rel = filepath.ToSlash(rel)
		if rel == "." {
			return nil
		}
		if d.IsDir() {
			localDirs = append(localDirs, rel)
			return nil
		}
		st, e2 := d.Info()
		if e2 != nil {
			return nil
		}
		localFiles = append(localFiles, localItem{rel: rel, size: st.Size()})
		localFileSet[rel] = true
		return nil
	}); err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		return res
	}

	// 预先创建本地存在的远端目录（自顶向下、幂等）。
	for _, d := range localDirs {
		if e := c.ensureDir("/" + d); e != nil {
			lg.Printf("[%s] 创建远端目录失败 %s: %v（上传可能受影响）", cfg.Name, d, e)
		}
	}

	// 逐文件比对远端大小，决定是否需要上传。
	var pending []localItem
	for _, it := range localFiles {
		rp := "/" + it.rel
		st, exists, e := c.statRemote(rp)
		if e != nil {
			lg.Printf("[%s] 检查远端失败 %s: %v", cfg.Name, rp, e)
			pending = append(pending, it)
			continue
		}
		if exists && st.size == it.size {
			res.skipped++
		} else {
			pending = append(pending, it)
		}
	}
	lg.Printf("[%s] 本地共 %d 个文件，待上传 %d，跳过 %d", cfg.Name, len(localFiles), len(pending), res.skipped)

	if cfg.DryRun {
		for _, it := range pending {
			lg.Printf("[%s][dry-run] 需上传: %s", cfg.Name, it.rel)
		}
		res.uploaded = len(pending)
		return res
	}

	var (
		wg    sync.WaitGroup
		sem   = make(chan struct{}, cfg.Concurrency)
		mu    sync.Mutex
		total = len(pending)
	)
	for _, it := range pending {
		wg.Add(1)
		go func(it localItem) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			rel := it.rel
			rp := "/" + rel
			parent := filepath.ToSlash(filepath.Dir(rp))
			if parent != "/" && parent != "." {
				if e := c.ensureDir(parent); e != nil {
					mu.Lock()
					res.failed++
					res.errors = append(res.errors, fmt.Sprintf("%s: 创建远端目录失败: %v", rel, e))
					mu.Unlock()
					lg.Printf("[%s] 上传前创建目录失败 %s: %v", cfg.Name, rel, e)
					return
				}
			}
			n, e := c.upload(filepath.Join(localRoot, filepath.FromSlash(rel)), rp, downloadRetries, lg)
			mu.Lock()
			defer mu.Unlock()
			if e != nil {
				res.failed++
				res.errors = append(res.errors, fmt.Sprintf("%s: %v", rel, e))
				lg.Printf("[%s] 上传失败 %s: %v", cfg.Name, rel, e)
				return
			}
			res.uploaded++
			res.bytesTransferred += n
			if res.uploaded%10 == 0 || res.uploaded == total {
				lg.Printf("[%s] 进度: %d/%d", cfg.Name, res.uploaded, total)
			}
		}(it)
	}
	wg.Wait()

	if cfg.Delete && res.ok() {
		rdirs, rfiles, werr := walkRemote(c)
		if werr != nil {
			lg.Printf("[%s] 列举远端以清理删除项失败: %v", cfg.Name, werr)
		} else {
		// 删除对端多出的文件
		for _, f := range rfiles {
			localRel := strings.TrimPrefix(f.path, "/")
			if !localFileSet[localRel] {
				if e := c.deleteRemote(f.path); e != nil {
					mu.Lock()
					res.failed++
					res.errors = append(res.errors, fmt.Sprintf("删除远端文件 %s 失败: %v", f.path, e))
					mu.Unlock()
					lg.Printf("[%s] 删除远端文件失败 %s: %v", cfg.Name, f.path, e)
				} else {
					mu.Lock()
					res.deleted++
					mu.Unlock()
				}
			}
		}
		// 删除对端多出的目录（自底向上，失败忽略）
		remoteDirRels := make([]string, 0, len(rdirs))
		for _, d := range rdirs {
			remoteDirRels = append(remoteDirRels, strings.TrimPrefix(d.path, "/"))
		}
		sort.Slice(remoteDirRels, func(i, j int) bool {
			return len(remoteDirRels[i]) > len(remoteDirRels[j])
		})
		localDirSet := map[string]bool{}
		for _, d := range localDirs {
			localDirSet[d] = true
		}
		for _, dr := range remoteDirRels {
			if !localDirSet[dr] {
				if e := c.deleteRemote("/" + dr); e == nil {
					mu.Lock()
					res.deleted++
					mu.Unlock()
				}
			}
		}
		}
	}

	mb := float64(res.bytesTransferred) / 1024 / 1024
	lg.Printf("[%s] 完成: 上传 %d, 跳过 %d, 删除 %d, 失败 %d, 传输 %.2f MB",
		cfg.Name, res.uploaded, res.skipped, res.deleted, res.failed, mb)
	return res
}

// copyTask 在远端 WebDAV 之间互传（source -> dest）：先以健壮的分块 Range 下载到本地中转区，
// 再流式 PUT 到目标端；目标端大小一致则跳过（增量）；delete 时清理目标端多余文件/目录。
func copyTask(cfg TaskConfig, lg *log.Logger) *result {
	if lg == nil {
		lg = log.Default()
	}
	res := &result{name: cfg.Name}
	srcEp := endpoint{URL: cfg.URL, Username: cfg.Username, Password: cfg.Password, NoVerifyTLS: cfg.NoVerifyTLS}
	dstEp := endpoint{URL: cfg.DstURL, Username: cfg.DstUsername, Password: cfg.DstPassword, NoVerifyTLS: cfg.DstNoVerifyTLS}
	src, err := newClient(cfg.Name, srcEp)
	if err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		lg.Printf("[%s] 失败(来源端): %v", cfg.Name, err)
		return res
	}
	dst, err := newClient(cfg.Name, dstEp)
	if err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		lg.Printf("[%s] 失败(目标端): %v", cfg.Name, err)
		return res
	}

	// 本地中转目录：未指定则用系统临时目录下的独立子目录。
	staging := filepath.Clean(expandEnv(cfg.Local))
	usingDefaultStaging := false
	if staging == "" || staging == "." || staging == string(filepath.Separator) {
		staging = filepath.Join(os.TempDir(), "webdav-sync-staging-"+sanitizeName(cfg.Name))
		usingDefaultStaging = true
	}
	if err := os.MkdirAll(staging, 0o755); err != nil {
		res.failed++
		res.errors = append(res.errors, fmt.Sprintf("创建中转目录失败: %v", err))
		return res
	}
	lg.Printf("[%s] 开始互传（WebDAV -> WebDAV）: %s -> %s（中转: %s）", cfg.Name, cfg.URL, cfg.DstURL, staging)

	srcDirs, srcFiles, err := walkRemote(src)
	if err != nil {
		res.failed++
		res.errors = append(res.errors, err.Error())
		lg.Printf("[%s] 遍历来源端失败: %v", cfg.Name, err)
		return res
	}
	lg.Printf("[%s] 来源端共 %d 个目录 / %d 个文件", cfg.Name, len(srcDirs), len(srcFiles))

	var pending []resource
	srcFileSet := map[string]bool{}
	for _, f := range srcFiles {
		srcFileSet[strings.TrimPrefix(f.path, "/")] = true
		rp := f.path
		st, exists, e := dst.statRemote(rp)
		if e != nil {
			lg.Printf("[%s] 检查目标端失败 %s: %v（将尝试上传）", cfg.Name, rp, e)
			pending = append(pending, f)
			continue
		}
		if exists && st.size == f.size {
			res.skipped++
		} else {
			pending = append(pending, f)
		}
	}
	lg.Printf("[%s] 待互传 %d，跳过 %d", cfg.Name, len(pending), res.skipped)

	if cfg.DryRun {
		for _, f := range pending {
			lg.Printf("[%s][dry-run] 需互传: %s", cfg.Name, f.path)
		}
		res.uploaded = len(pending)
		return res
	}

	// 预建目标端目录（自顶向下、幂等）。
	for _, d := range srcDirs {
		if e := dst.ensureDir(d.path); e != nil {
			lg.Printf("[%s] 创建目标端目录失败 %s: %v（互传可能受影响）", cfg.Name, d.path, e)
		}
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
			relLocal := filepath.FromSlash(strings.TrimPrefix(f.path, "/"))
			tmp := filepath.Join(staging, relLocal)
			// 1) 从来源端下载到本地中转（健壮分块 Range，自动续传）。
			n, e := src.download(f, tmp, downloadRetries, lg)
			if e != nil {
				mu.Lock()
				res.failed++
				res.errors = append(res.errors, fmt.Sprintf("%s: 下载失败: %v", f.path, e))
				mu.Unlock()
				lg.Printf("[%s] 互传下载失败 %s: %v", cfg.Name, f.path, e)
				return
			}
			// 2) 上传到目标端。
			rp := f.path
			parent := filepath.ToSlash(filepath.Dir(rp))
			if parent != "/" && parent != "." {
				if e := dst.ensureDir(parent); e != nil {
					mu.Lock()
					res.failed++
					res.errors = append(res.errors, fmt.Sprintf("%s: 创建目标目录失败: %v", f.path, e))
					mu.Unlock()
					lg.Printf("[%s] 互传前创建目录失败 %s: %v", cfg.Name, f.path, e)
					_ = os.Remove(tmp)
					return
				}
			}
			if _, e := dst.upload(tmp, rp, downloadRetries, lg); e != nil {
				mu.Lock()
				res.failed++
				res.errors = append(res.errors, fmt.Sprintf("%s: 上传失败: %v", f.path, e))
				mu.Unlock()
				lg.Printf("[%s] 互传上传失败 %s: %v", cfg.Name, f.path, e)
				_ = os.Remove(tmp)
				return
			}
			mu.Lock()
			res.uploaded++
			res.bytesTransferred += n
			mu.Unlock()
			_ = os.Remove(tmp)
			if res.uploaded%10 == 0 || res.uploaded == total {
				lg.Printf("[%s] 进度: %d/%d", cfg.Name, res.uploaded, total)
			}
		}(f)
	}
	wg.Wait()

	if cfg.Delete && res.ok() {
		rdirs, rfiles, werr := walkRemote(dst)
		if werr != nil {
			lg.Printf("[%s] 列举目标端以清理删除项失败: %v", cfg.Name, werr)
		} else {
			for _, f := range rfiles {
				localRel := strings.TrimPrefix(f.path, "/")
				if !srcFileSet[localRel] {
					if e := dst.deleteRemote(f.path); e != nil {
						mu.Lock()
						res.failed++
						res.errors = append(res.errors, fmt.Sprintf("删除目标文件 %s 失败: %v", f.path, e))
						mu.Unlock()
						lg.Printf("[%s] 删除目标文件失败 %s: %v", cfg.Name, f.path, e)
					} else {
						mu.Lock()
						res.deleted++
						mu.Unlock()
					}
				}
			}
			remoteDirRels := make([]string, 0, len(rdirs))
			for _, d := range rdirs {
				remoteDirRels = append(remoteDirRels, strings.TrimPrefix(d.path, "/"))
			}
			sort.Slice(remoteDirRels, func(i, j int) bool {
				return len(remoteDirRels[i]) > len(remoteDirRels[j])
			})
			for _, dr := range remoteDirRels {
				if !srcFileSet[dr] {
					if e := dst.deleteRemote("/" + dr); e == nil {
						mu.Lock()
						res.deleted++
						mu.Unlock()
					}
				}
			}
		}
	}

	// 清理中转区（仅清理我们自建的临时目录，用户指定的中转目录保留）。
	if usingDefaultStaging {
		_ = os.RemoveAll(staging)
	}

	mb := float64(res.bytesTransferred) / 1024 / 1024
	lg.Printf("[%s] 完成: 互传 %d, 跳过 %d, 删除 %d, 失败 %d, 传输 %.2f MB",
		cfg.Name, res.uploaded, res.skipped, res.deleted, res.failed, mb)
	return res
}

// sanitizeName 把任务名清洗为可用作目录名的字符串。
func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' || r == '.' {
			b.WriteRune(r)
		} else {
			b.WriteRune('_')
		}
	}
	if b.Len() == 0 {
		b.WriteString("task")
	}
	return b.String()
}

// runSync 根据 direction 选择下载(pull)/上传(push)/互传(copy)。
func runSync(cfg TaskConfig, lg *log.Logger) *result {
	switch cfg.Direction {
	case "push":
		return pushTask(cfg, lg)
	case "copy":
		return copyTask(cfg, lg)
	default:
		return syncTask(cfg, lg)
	}
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
	// 先展开服务器档案中的环境变量，供任务引用时合并。
	for i := range cfg.Servers {
		s := &cfg.Servers[i]
		s.URL = expandEnv(s.URL)
		s.Username = expandEnv(s.Username)
		s.Password = expandEnv(s.Password)
	}
	for i := range cfg.Tasks {
		t := &cfg.Tasks[i]
		if t.Name == "" {
			t.Name = fmt.Sprintf("task-%d", i+1)
		}
		switch t.Direction {
		case "push", "copy":
			// 保留
		default:
			t.Direction = "pull" // 默认下载；push=上传；copy=互传
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
		t.DstURL = expandEnv(t.DstURL)
		t.DstUsername = expandEnv(t.DstUsername)
		t.DstPassword = expandEnv(t.DstPassword)
		// 引用服务器档案：把档案的 url/账号/密码/TLS 合并进来（任务自带非空字段优先）。
		*t = mergeServer(*t, cfg.Servers)
		*t = mergeDstServer(*t, cfg.Servers)
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
		concurrency = flag.Int("concurrency", 0, "并发传输数 (默认 8)")
		deleteFlag  = flag.Bool("delete", false, "删除对端已不存在的文件 (pull 删本地 / push,copy 删目标端)")
		direction   = flag.String("direction", "pull", "同步方向: pull(下载, 远端→本地) / push(上传, 本地→远端) / copy(互传, 远端→远端)")
		noVerify    = flag.Bool("no-verify-tls", false, "跳过 TLS 证书校验")
		dryRun      = flag.Bool("dry-run", false, "只打印将要传输的文件，不实际传输")
		// 目标端（push/copy 使用）
		dstURL      = flag.String("dst-url", "", "目标端 WebDAV URL (copy/push)")
		dstUser     = flag.String("dst-username", "", "目标端用户名")
		dstPass     = flag.String("dst-password", "", "目标端密码")
		dstServer   = flag.String("dst-server", "", "目标端引用的服务器档案名")
		dstNoVerify = flag.Bool("dst-no-verify-tls", false, "目标端跳过 TLS 校验")
		web         = flag.Bool("web", false, "启动 Web 管理界面 (多任务管理)")
		addr        = flag.String("addr", ":8080", "Web 服务监听地址 (配合 --web)")
		webAuth     = flag.String("web-auth", "", "Web 界面 Basic Auth，格式 user:pass (可选)")
		interval    = flag.Int("interval", 0, "Web 模式下自动同步间隔秒数 (0=关闭，配合 --web)")
	)
	flag.Parse()

	// Web 登录凭据支持环境变量 WEB_AUTH（等价于 --web-auth 用户名:密码），便于容器部署。
	if *webAuth == "" {
		*webAuth = os.Getenv("WEB_AUTH")
	}

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
		if *direction != "" {
			for i := range tasks {
				tasks[i].Direction = *direction
			}
		}
	} else {
		u := orEnv(*urlFlag, "WEBDAV_URL")
		l := orEnv(*localFlag, "WEBDAV_LOCAL_DIR")
		dir := *direction
		if dir == "" {
			dir = "pull"
		}
		// 校验单任务所需的端点。
		switch dir {
		case "push":
			if l == "" {
				log.Fatalf("push 需要 --local（本地来源目录）")
			}
			if *dstURL == "" && *dstServer == "" {
				log.Fatalf("push 需要 --dst-url 或 --dst-server（目标端）")
			}
		case "copy":
			if u == "" {
				log.Fatalf("copy 需要 --url（来源端 WebDAV）")
			}
			if *dstURL == "" && *dstServer == "" {
				log.Fatalf("copy 需要 --dst-url 或 --dst-server（目标端）")
			}
		default: // pull
			if u == "" || l == "" {
				log.Fatalf("需要 --config，或同时提供 --url 与 --local（也可用环境变量 WEBDAV_URL / WEBDAV_LOCAL_DIR）")
			}
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
			Name:           "default",
			URL:            expandEnv(u),
			Local:          expandEnv(l),
			Username:       orEnv(*username, "WEBDAV_USERNAME"),
			Password:       orEnv(*password, "WEBDAV_PASSWORD"),
			Direction:      dir,
			Concurrency:    conc,
			Delete:         *deleteFlag,
			NoVerifyTLS:    *noVerify,
			DryRun:         *dryRun,
			DstServer:      *dstServer,
			DstURL:         expandEnv(*dstURL),
			DstUsername:    *dstUser,
			DstPassword:    *dstPass,
			DstNoVerifyTLS: *dstNoVerify,
		}}
	}

	failed := false
	lg := log.New(os.Stderr, "", log.LstdFlags)
	for _, t := range tasks {
		if t.Direction == "push" {
			if t.Local == "" {
				log.Printf("[%s] push 缺少 local 配置", t.Name)
				failed = true
				continue
			}
			if t.DstURL == "" && t.DstServer == "" {
				log.Printf("[%s] push 缺少目标端配置", t.Name)
				failed = true
				continue
			}
		} else if t.Direction == "copy" {
			if t.URL == "" && t.Server == "" {
				log.Printf("[%s] copy 缺少来源端配置", t.Name)
				failed = true
				continue
			}
			if t.DstURL == "" && t.DstServer == "" {
				log.Printf("[%s] copy 缺少目标端配置", t.Name)
				failed = true
				continue
			}
		} else { // pull
			if t.URL == "" || t.Local == "" {
				log.Printf("[%s] 缺少 url 或 local 配置", t.Name)
				failed = true
				continue
			}
		}
		if r := runSync(t, lg); !r.ok() {
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
