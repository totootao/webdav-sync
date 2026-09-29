package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// 任务运行状态
// ---------------------------------------------------------------------------

type TaskStatus string

const (
	StatusIdle    TaskStatus = "idle"
	StatusRunning TaskStatus = "running"
	StatusSuccess TaskStatus = "success"
	StatusFailed  TaskStatus = "failed"
)

// TaskInfo 是 TaskConfig 加上运行态信息。
type TaskInfo struct {
	TaskConfig
	HasPassword bool       `json:"has_password"`
	Status      TaskStatus `json:"status"`
	LastRun     *time.Time `json:"last_run,omitempty"`
	LastMessage string     `json:"last_message,omitempty"`
	LastStats   *RunStats  `json:"last_stats,omitempty"`
}

// RunStats 任务最近一次运行的统计（用于 JSON 展示）。
type RunStats struct {
	Downloaded int      `json:"downloaded"`
	Skipped    int      `json:"skipped"`
	Deleted    int      `json:"deleted"`
	Failed     int      `json:"failed"`
	Bytes      int64    `json:"bytes"`
	Errors     []string `json:"errors,omitempty"`
}

const logMax = 3000

// ringLog 是一个带上限的环形日志缓冲，实现 io.Writer。
type ringLog struct {
	mu  sync.Mutex
	buf []string
}

func (l *ringLog) Write(p []byte) (int, error) {
	s := strings.TrimRight(string(p), "\n")
	if s == "" {
		return len(p), nil
	}
	lines := strings.Split(s, "\n")
	l.mu.Lock()
	for _, line := range lines {
		l.buf = append(l.buf, line)
		if len(l.buf) > logMax {
			l.buf = l.buf[len(l.buf)-logMax:]
		}
	}
	l.mu.Unlock()
	return len(p), nil
}

func (l *ringLog) Lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]string, len(l.buf))
	copy(out, l.buf)
	return out
}

func (l *ringLog) Reset() {
	l.mu.Lock()
	l.buf = nil
	l.mu.Unlock()
}

// broadcastLog 在 ringLog 基础上把每一行推送给所有 SSE 订阅者。
type broadcastLog struct {
	ringLog
	subsMu sync.Mutex
	subs   map[chan string]struct{}
}

func newBroadcastLog() *broadcastLog {
	return &broadcastLog{subs: map[chan string]struct{}{}}
}

func (b *broadcastLog) Write(p []byte) (int, error) {
	n, err := b.ringLog.Write(p)
	if err != nil {
		return n, err
	}
	s := strings.TrimRight(string(p), "\n")
	if s == "" {
		return n, nil
	}
	b.subsMu.Lock()
	defer b.subsMu.Unlock()
	for _, line := range strings.Split(s, "\n") {
		for ch := range b.subs {
			select {
			case ch <- line:
			default:
			}
		}
	}
	return n, nil
}

func (b *broadcastLog) Subscribe(ch chan string) {
	b.subsMu.Lock()
	b.subs[ch] = struct{}{}
	b.subsMu.Unlock()
}

func (b *broadcastLog) Unsubscribe(ch chan string) {
	b.subsMu.Lock()
	delete(b.subs, ch)
	b.subsMu.Unlock()
}

// ---------------------------------------------------------------------------
// 任务存储
// ---------------------------------------------------------------------------

type TaskStore struct {
	mu     sync.Mutex
	path   string
	config Config
	infos  map[string]*TaskInfo
	logs   map[string]*ringLog // 每个任务最近一次运行的日志
	global *broadcastLog
}

func newStore(path string) *TaskStore {
	return &TaskStore{
		path:   path,
		infos:  map[string]*TaskInfo{},
		logs:   map[string]*ringLog{},
		global: newBroadcastLog(),
	}
}

func (s *TaskStore) load() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	data, err := os.ReadFile(s.path)
	if err != nil {
		if os.IsNotExist(err) {
			s.config = Config{Concurrency: 8}
			s.rebuildInfosLocked()
			return nil
		}
		return err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return fmt.Errorf("配置文件解析失败: %w", err)
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 8
	}
	s.config = cfg
	s.rebuildInfosLocked()
	return nil
}

func (s *TaskStore) rebuildInfosLocked() {
	newInfos := map[string]*TaskInfo{}
	for i := range s.config.Tasks {
		t := s.config.Tasks[i]
		if t.Name == "" {
			t.Name = fmt.Sprintf("task-%d", i+1)
		}
		if t.Concurrency <= 0 {
			t.Concurrency = s.config.Concurrency
		}
		t.Delete = t.Delete || s.config.Delete
		if s.config.NoVerifyTLS {
			t.NoVerifyTLS = true
		}
		info := &TaskInfo{TaskConfig: t, Status: StatusIdle}
		if old, ok := s.infos[t.Name]; ok {
			info.Status = old.Status
			info.LastRun = old.LastRun
			info.LastMessage = old.LastMessage
			info.LastStats = old.LastStats
			info.HasPassword = old.HasPassword
		}
		if t.Password != "" {
			info.HasPassword = true
		}
		newInfos[t.Name] = info
	}
	s.infos = newInfos
}

// save 将当前 config 写盘。调用方必须已持有 s.mu。
func (s *TaskStore) save() error {
	data, err := json.MarshalIndent(s.config, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.path, data, 0o644)
}

// PublicTasks 返回脱敏（密码置空）的任务列表，按名称排序。
func (s *TaskStore) PublicTasks() []TaskInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]TaskInfo, 0, len(s.infos))
	for _, info := range s.infos {
		c := info.TaskConfig
		c.Password = ""
		out = append(out, TaskInfo{
			TaskConfig:  c,
			HasPassword: info.HasPassword,
			Status:      info.Status,
			LastRun:     info.LastRun,
			LastMessage: info.LastMessage,
			LastStats:   info.LastStats,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// PublicConfig 返回顶层默认配置。
func (s *TaskStore) PublicConfig() Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Config{
		Concurrency: s.config.Concurrency,
		Delete:      s.config.Delete,
		NoVerifyTLS: s.config.NoVerifyTLS,
	}
}

// AddTask 新增任务并持久化。
func (s *TaskStore) AddTask(t TaskConfig) error {
	if err := validateTask(t); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range s.config.Tasks {
		if e.Name == t.Name {
			return fmt.Errorf("任务名 %q 已存在", t.Name)
		}
	}
	s.config.Tasks = append(s.config.Tasks, t)
	s.rebuildInfosLocked()
	return s.save()
}

// UpdateTask 更新已有任务；密码为空时保留原密码。
func (s *TaskStore) UpdateTask(name string, t TaskConfig) error {
	if err := validateTask(t); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i, e := range s.config.Tasks {
		if e.Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("任务 %q 不存在", name)
	}
	if t.Password == "" {
		t.Password = s.config.Tasks[idx].Password
	}
	if name != t.Name {
		for _, e := range s.config.Tasks {
			if e.Name == t.Name {
				return fmt.Errorf("任务名 %q 已存在", t.Name)
			}
		}
	}
	s.config.Tasks[idx] = t
	s.rebuildInfosLocked()
	return s.save()
}

// DeleteTask 删除任务并持久化。
func (s *TaskStore) DeleteTask(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	idx := -1
	for i, e := range s.config.Tasks {
		if e.Name == name {
			idx = i
			break
		}
	}
	if idx < 0 {
		return fmt.Errorf("任务 %q 不存在", name)
	}
	s.config.Tasks = append(s.config.Tasks[:idx], s.config.Tasks[idx+1:]...)
	delete(s.infos, name)
	delete(s.logs, name)
	return s.save()
}

func validateTask(t TaskConfig) error {
	if t.Name == "" {
		return fmt.Errorf("name 不能为空")
	}
	u, err := url.Parse(expandEnv(t.URL))
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("url 无效（需 http/https 且含主机）: %q", t.URL)
	}
	if t.Local == "" {
		return fmt.Errorf("local 不能为空")
	}
	return nil
}

// UpdateConfig 更新顶层默认配置并持久化。
func (s *TaskStore) UpdateConfig(c Config) error {
	s.mu.Lock()
	if c.Concurrency <= 0 {
		c.Concurrency = 8
	}
	s.config.Concurrency = c.Concurrency
	s.config.Delete = c.Delete
	s.config.NoVerifyTLS = c.NoVerifyTLS
	s.rebuildInfosLocked()
	err := s.save()
	s.mu.Unlock()
	return err
}

// Run 同步执行单个任务，记录状态、统计与日志。
func (s *TaskStore) Run(name string) (*RunStats, error) {
	s.mu.Lock()
	info, ok := s.infos[name]
	if !ok {
		s.mu.Unlock()
		return nil, fmt.Errorf("任务 %q 不存在", name)
	}
	if info.Status == StatusRunning {
		s.mu.Unlock()
		return nil, fmt.Errorf("任务正在运行中")
	}
	cfg := info.TaskConfig
	info.Status = StatusRunning
	info.LastMessage = "运行中…"
	info.LastStats = nil
	s.mu.Unlock()

	runBuf := &ringLog{}
	now := time.Now()
	lg := log.New(io.MultiWriter(os.Stdout, s.global, runBuf), "", log.LstdFlags)

	// 运行前展开环境变量，保持配置中的 ${ENV} 占位符不被写回。
	cfg2 := cfg
	cfg2.URL = expandEnv(cfg.URL)
	cfg2.Local = expandEnv(cfg.Local)
	cfg2.Username = expandEnv(cfg.Username)
	cfg2.Password = expandEnv(cfg.Password)

	res := syncTask(cfg2, lg)
	stats := &RunStats{
		Downloaded: res.downloaded,
		Skipped:    res.skipped,
		Deleted:    res.deleted,
		Failed:     res.failed,
		Bytes:      res.bytesTransferred,
		Errors:     res.errors,
	}

	s.mu.Lock()
	info.LastRun = &now
	info.LastStats = stats
	if res.ok() {
		info.Status = StatusSuccess
		info.LastMessage = fmt.Sprintf("成功：下载 %d / 跳过 %d / 失败 %d", res.downloaded, res.skipped, res.failed)
	} else {
		info.Status = StatusFailed
		info.LastMessage = fmt.Sprintf("失败：%d 个文件出错", res.failed)
	}
	s.logs[name] = runBuf
	s.mu.Unlock()
	return stats, nil
}

// RunAsync 在后台执行任务，立即返回。
func (s *TaskStore) RunAsync(name string) error {
	s.mu.Lock()
	info, ok := s.infos[name]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("任务 %q 不存在", name)
	}
	if info.Status == StatusRunning {
		s.mu.Unlock()
		return fmt.Errorf("任务正在运行中")
	}
	s.mu.Unlock()
	go func() {
		_, _ = s.Run(name)
	}()
	return nil
}

// Logs 返回某任务最近一次运行的日志行。
func (s *TaskStore) Logs(name string) []string {
	s.mu.Lock()
	l, ok := s.logs[name]
	s.mu.Unlock()
	if !ok {
		return nil
	}
	return l.Lines()
}

// GlobalLogs 返回全局最近日志行。
func (s *TaskStore) GlobalLogs() []string {
	return s.global.Lines()
}

// AutoLoop 每 interval 秒自动同步全部任务（若已运行则跳过）。
func (s *TaskStore) AutoLoop(interval int) {
	if interval <= 0 {
		return
	}
	ticker := time.NewTicker(time.Duration(interval) * time.Second)
	for range ticker.C {
		s.mu.Lock()
		names := make([]string, 0, len(s.infos))
		for n := range s.infos {
			names = append(names, n)
		}
		s.mu.Unlock()
		for _, n := range names {
			if err := s.RunAsync(n); err != nil {
				// 正在运行或不存在，忽略
				continue
			}
		}
	}
}
