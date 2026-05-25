package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"
)

type Settings struct {
	Validation    ValidationSettings `json:"validation"`
	Protocols     []string           `json:"protocols"`
	ProtocolOrder []string           `json:"protocol_order"`
	Base64Links   []string           `json:"base64_links"`
	TextLinks     []string           `json:"text_links"`
	Output        OutputSettings     `json:"output"`
}

type ValidationSettings struct {
	NumWorkers             int      `json:"num_workers"`
	GlobalTimeoutSec       float64  `json:"global_timeout_sec"`
	SingboxStartTimeoutMs  int      `json:"singbox_start_timeout_ms"`
	SingboxStartIntervalMs int      `json:"singbox_start_interval_ms"`
	HTTPRequestTimeoutMs   int      `json:"http_request_timeout_ms"`
	HTTPDialTimeoutMs      int      `json:"http_dial_timeout_ms"`
	HTTPResponseTimeoutMs  int      `json:"http_response_timeout_ms"`
	PortCheckTimeoutMs     int      `json:"port_check_timeout_ms"`
	MaxRetries             int      `json:"max_retries"`
	BasePort               int      `json:"base_port"`
	BatchRestMs            int      `json:"batch_rest_ms"`
	ProcessKillWaitMs      int      `json:"process_kill_wait_ms"`
	FetchRetryCount        int      `json:"fetch_retry_count"`
	FetchRetryDelayMs      int      `json:"fetch_retry_delay_ms"`
	TestURLs               []string `json:"test_urls"`
}

type OutputSettings struct {
	ConfigName   string `json:"config_name"`
	MainFile     string `json:"main_file"`
	ProtocolsDir string `json:"protocols_dir"`
}

type ClashWSOpts struct {
	Path    string            `yaml:"path"`
	Headers map[string]string `yaml:"headers"`
}

type ClashGRPCOpts struct {
	ServiceName string `yaml:"grpc-service-name"`
}

type ClashH2Opts struct {
	Path []string `yaml:"path"`
	Host []string `yaml:"host"`
}

type ClashHTTPOpts struct {
	Method  string              `yaml:"method"`
	Path    []string            `yaml:"path"`
	Headers map[string][]string `yaml:"headers"`
}

type ClashHTTPUpgradeOpts struct {
	Path    string            `yaml:"path"`
	Host    string            `yaml:"host"`
	Headers map[string]string `yaml:"headers"`
}

type ClashSplitHTTPOpts struct {
	Path string `yaml:"path"`
	Host string `yaml:"host"`
}

type ClashRealityOpts struct {
	PublicKey string `yaml:"public-key"`
	ShortID   string `yaml:"short-id"`
}

type ClashProxy struct {
	Name   string      `yaml:"name"`
	Type   string      `yaml:"type"`
	Server string      `yaml:"server"`
	Port   interface{} `yaml:"port"`

	UUID     string      `yaml:"uuid"`
	Password string      `yaml:"password"`
	AlterID  interface{} `yaml:"alterId"`
	Cipher   string      `yaml:"cipher"`

	TLS            bool     `yaml:"tls"`
	SkipCertVerify bool     `yaml:"skip-cert-verify"`
	SNI            string   `yaml:"servername"`
	SniAlt         string   `yaml:"sni"`
	Fingerprint    string   `yaml:"client-fingerprint"`
	FingerprintAlt string   `yaml:"fingerprint"`
	ALPN           []string `yaml:"alpn"`

	Network           string                `yaml:"network"`
	WSOpts            *ClashWSOpts          `yaml:"ws-opts"`
	GRPCOpts          *ClashGRPCOpts        `yaml:"grpc-opts"`
	H2Opts            *ClashH2Opts          `yaml:"h2-opts"`
	HTTPOpts          *ClashHTTPOpts        `yaml:"http-opts"`
	HTTPUpgradeOpts   *ClashHTTPUpgradeOpts `yaml:"httpupgrade-opts"`
	SplitHTTPOpts     *ClashSplitHTTPOpts   `yaml:"splithttp-opts"`

	Flow        string            `yaml:"flow"`
	RealityOpts *ClashRealityOpts `yaml:"reality-opts"`

	Plugin     string                 `yaml:"plugin"`
	PluginOpts map[string]interface{} `yaml:"plugin-opts"`

	AuthStr    string      `yaml:"auth-str"`
	AuthStrAlt string      `yaml:"auth_str"`
	Auth       string      `yaml:"auth"`
	Up         interface{} `yaml:"up"`
	Down       interface{} `yaml:"down"`

	Obfs         string `yaml:"obfs"`
	ObfsPassword string `yaml:"obfs-password"`

	Token string `yaml:"token"`

	Protocol      string `yaml:"protocol"`
	ObfsParam     string `yaml:"obfs-param"`
	ProtocolParam string `yaml:"protocol-param"`
}

type clashConfigWrapper struct {
	Proxies    []ClashProxy `yaml:"proxies"`
	ProxiesOld []ClashProxy `yaml:"Proxy"`
	ProxiesP   []ClashProxy `yaml:"proxy"`
}

var cfg Settings
var fetchHTTPClient = &http.Client{
	Transport: &http.Transport{
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   10,
		IdleConnTimeout:       15 * time.Second,
		ResponseHeaderTimeout: 4 * time.Second,
		DisableKeepAlives:     false,
	},
}

// SNI replacement constants
const sniHost = "127.0.0.1"
const sniPort = 40443

type protoStat struct {
	mu         sync.Mutex
	tested     int
	passed     int
	parseFail  int
	startFail  int
	connFail   int
	totalLatMs int64
}

type batchTracker struct {
	mu   sync.Mutex
	cmds []*exec.Cmd
}

func (bt *batchTracker) register(cmd *exec.Cmd) {
	bt.mu.Lock()
	bt.cmds = append(bt.cmds, cmd)
	bt.mu.Unlock()
}

func (bt *batchTracker) killAll() {
	bt.mu.Lock()
	cmds := make([]*exec.Cmd, len(bt.cmds))
	copy(cmds, bt.cmds)
	bt.cmds = bt.cmds[:0]
	bt.mu.Unlock()

	var wg sync.WaitGroup
	for _, cmd := range cmds {
		cmd := cmd
		wg.Add(1)
		go func() {
			defer wg.Done()
			if cmd.Process == nil {
				return
			}
			pid := cmd.Process.Pid
			if pgid, err := syscall.Getpgid(pid); err == nil {
				syscall.Kill(-pgid, syscall.SIGKILL)
			}
			cmd.Process.Kill()
			done := make(chan struct{})
			go func() {
				cmd.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(2 * time.Second):
				if pgid, err := syscall.Getpgid(pid); err == nil {
					syscall.Kill(-pgid, syscall.SIGKILL)
				}
				syscall.Kill(pid, syscall.SIGKILL)
			}
		}()
	}
	wg.Wait()
}

type Logger struct {
	mu         sync.Mutex
	file       *os.File
	buf        *bufio.Writer
	passed     int64
	parseFail  int64
	startFail  int64
	connFail   int64
	totalTest  int64
	protoStats map[string]*protoStat
}

var gLog *Logger

func newLogger(dir string) (*Logger, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	ts := time.Now().Format("2006-01-02_15-04-05")
	f, err := os.Create(filepath.Join(dir, "validation_"+ts+".log"))
	if err != nil {
		return nil, err
	}
	return &Logger{
		file:       f,
		buf:        bufio.NewWriterSize(f, 256*1024),
		protoStats: make(map[string]*protoStat),
	}, nil
}

func (l *Logger) writeLine(s string) {
	l.mu.Lock()
	l.buf.WriteString(s)
	l.buf.WriteByte('\n')
	l.mu.Unlock()
}

func (l *Logger) logStart(fetched, failedSrc int) {
	l.writeLine("==========================================================")
	l.writeLine("  VALIDATION RUN STARTED")
	l.writeLine(fmt.Sprintf("  Time      : %s", time.Now().Format("2006-01-02 15:04:05 MST")))
	l.writeLine(fmt.Sprintf("  Workers   : %d", cfg.Validation.NumWorkers))
	l.writeLine(fmt.Sprintf("  Timeout   : %.0fs per config", cfg.Validation.GlobalTimeoutSec))
	l.writeLine(fmt.Sprintf("  Fetched   : %d  |  FailedSrc: %d", fetched, failedSrc))
	l.writeLine("==========================================================")
	l.writeLine("")
}

func (l *Logger) logProtoStart(proto string, count int) {
	l.mu.Lock()
	if _, ok := l.protoStats[proto]; !ok {
		l.protoStats[proto] = &protoStat{}
	}
	l.mu.Unlock()
	l.writeLine(fmt.Sprintf("--- PROTOCOL: %s (%d unique) ---", strings.ToUpper(proto), count))
}

func (l *Logger) logResult(idx int64, proto, configURL string, res validationResult) {
	l.mu.Lock()
	st := l.protoStats[proto]
	if st == nil {
		st = &protoStat{}
		l.protoStats[proto] = st
	}
	l.mu.Unlock()

	st.mu.Lock()
	st.tested++
	if res.passed {
		st.passed++
		st.totalLatMs += res.latency.Milliseconds()
		atomic.AddInt64(&l.passed, 1)
	} else if strings.HasPrefix(res.failReason, "PARSE:") {
		st.parseFail++
		atomic.AddInt64(&l.parseFail, 1)
	} else if strings.HasPrefix(res.failReason, "SINGBOX_START:") || strings.HasPrefix(res.failReason, "START:") {
		st.startFail++
		atomic.AddInt64(&l.startFail, 1)
	} else {
		st.connFail++
		atomic.AddInt64(&l.connFail, 1)
	}
	atomic.AddInt64(&l.totalTest, 1)
	st.mu.Unlock()

	ts := time.Now().Format("15:04:05.000")
	if res.passed {
		l.writeLine(fmt.Sprintf("[%s] PASS  [%5d] %-6s lat=%dms  %s",
			ts, idx, proto, res.latency.Milliseconds(), truncate(configURL, 120)))
	} else {
		l.writeLine(fmt.Sprintf("[%s] FAIL  [%5d] %-6s %s  |  %s",
			ts, idx, proto, truncate(res.failReason, 80), truncate(configURL, 60)))
	}
}

func (l *Logger) logSummary(duration float64, results []configResult, failedLinks []string) {
	byProto := make(map[string]int)
	for _, r := range results {
		byProto[r.proto]++
	}

	l.writeLine("")
	l.writeLine("==========================================================")
	l.writeLine("  SUMMARY")
	l.writeLine("==========================================================")
	l.writeLine(fmt.Sprintf("  Duration    : %.2fs", duration))
	l.writeLine(fmt.Sprintf("  Total Tested: %d", atomic.LoadInt64(&l.totalTest)))
	l.writeLine(fmt.Sprintf("  Passed      : %d", atomic.LoadInt64(&l.passed)))
	l.writeLine(fmt.Sprintf("  Parse Fail  : %d", atomic.LoadInt64(&l.parseFail)))
	l.writeLine(fmt.Sprintf("  Start Fail  : %d", atomic.LoadInt64(&l.startFail)))
	l.writeLine(fmt.Sprintf("  Conn Fail   : %d", atomic.LoadInt64(&l.connFail)))
	l.writeLine("")
	l.writeLine("  Per-Protocol Breakdown:")
	l.writeLine(fmt.Sprintf("  %-6s  %6s  %6s  %7s  %9s  %9s  %9s  %8s",
		"Proto", "Tested", "Passed", "Pass%", "ParseFail", "StartFail", "ConnFail", "AvgLat"))

	for _, p := range cfg.ProtocolOrder {
		st := l.protoStats[p]
		if st == nil {
			continue
		}
		passRate := 0.0
		avgLat := int64(0)
		if st.tested > 0 {
			passRate = float64(st.passed) / float64(st.tested) * 100
		}
		if st.passed > 0 {
			avgLat = st.totalLatMs / int64(st.passed)
		}
		l.writeLine(fmt.Sprintf("  %-6s  %6d  %6d  %6.1f%%  %9d  %9d  %9d  %7dms",
			p, st.tested, st.passed, passRate, st.parseFail, st.startFail, st.connFail, avgLat))
	}

	tt := atomic.LoadInt64(&l.totalTest)
	if tt > 0 {
		overall := float64(atomic.LoadInt64(&l.passed)) / float64(tt) * 100
		l.writeLine(fmt.Sprintf("\n  Overall pass rate: %.1f%%", overall))
	}

	if len(failedLinks) > 0 {
		l.writeLine("\n  Failed Sources:")
		for _, fl := range failedLinks {
			l.writeLine("    - " + fl)
		}
	}

	l.writeLine("\n  Output Files:")
	for _, p := range cfg.ProtocolOrder {
		if n := byProto[p]; n > 0 {
			l.writeLine(fmt.Sprintf("    %-6s: %d → %s/%s.txt | %s/%s_clash.yaml | %s/%s_clash_advanced.yaml",
				p, n, cfg.Output.ProtocolsDir, p, cfg.Output.ProtocolsDir, p, cfg.Output.ProtocolsDir, p))
		}
	}
	l.writeLine(fmt.Sprintf("  Total  : %d → %s | clash.yaml | clash_advanced.yaml", len(results), cfg.Output.MainFile))
	l.writeLine("==========================================================")
}

func (l *Logger) close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.buf != nil {
		l.buf.Flush()
	}
	if l.file != nil {
		l.file.Close()
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

type clashBase struct {
	simple   string
	advanced string
}

var gClash clashBase

var gInputByProto = make(map[string]int)

func loadClashBase(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("clash_base.yaml: %w", err)
	}
	gClash.simple = string(data)
	return nil
}

func loadClashBaseAdvanced(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("clash_base_advanced.yaml: %w", err)
	}
	gClash.advanced = string(data)
	return nil
}

func injectClashProxies(baseContent string, proxyEntries []string, proxyNames []string) string {
	const proxiesPlaceholder = "# ---PROXIES---\n"
	const namesPlaceholder = "# ---PROXY-NAMES---\n"

	var proxyBlock strings.Builder
	for _, e := range proxyEntries {
		proxyBlock.WriteString(e)
	}

	var namesBlock strings.Builder
	for _, n := range proxyNames {
		fmt.Fprintf(&namesBlock, "      - %s\n", yamlQuote(n))
	}

	result := strings.ReplaceAll(baseContent, proxiesPlaceholder, proxyBlock.String())
	result = strings.ReplaceAll(result, namesPlaceholder, namesBlock.String())
	return result
}

func loadSettings(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, &cfg)
}

type fetchResult struct {
	url        string
	content    string
	statusCode int
	err        error
}

type validationResult struct {
	passed     bool
	latency    time.Duration
	failReason string
}

type configResult struct {
	line  string
	proto string
}

func loadSubsFromFile(path string) []string {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var urls []string
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "#") {
			urls = append(urls, line)
		}
	}
	return urls
}

func isLikelyBase64(s string) bool {
	s = strings.TrimRight(strings.TrimSpace(s), "=")
	if len(s) < 16 {
		return false
	}
	valid := 0
	for _, c := range s {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
			(c >= '0' && c <= '9') || c == '+' || c == '/' || c == '-' || c == '_' {
			valid++
		} else if c == '\n' || c == '\r' || c == ' ' || c == '\t' {
			continue
		} else {
			return false
		}
	}
	return valid > len(s)/2
}

func hasProtoPrefix(s string) bool {
	protos := []string{"vmess://", "vless://", "trojan://", "ss://", "ssr://",
		"hy2://", "hysteria2://", "hy://", "hysteria://", "tuic://"}
	for _, p := range protos {
		if strings.Contains(s, p) {
			return true
		}
	}
	return false
}

func extractLines(content string) []string {
	var lines []string
	for _, line := range strings.Split(content, "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func clashPortStr(v interface{}) string {
	if v == nil {
		return "443"
	}
	switch x := v.(type) {
	case int:
		return strconv.Itoa(x)
	case float64:
		return strconv.Itoa(int(x))
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return "443"
		}
		return s
	}
	return "443"
}

func clashBandwidthMbps(v interface{}) int {
	if v == nil {
		return 10
	}
	switch x := v.(type) {
	case int:
		if x <= 0 {
			return 10
		}
		return x
	case float64:
		if int(x) <= 0 {
			return 10
		}
		return int(x)
	case string:
		s := strings.ToLower(strings.TrimSpace(x))
		for _, suffix := range []string{" mbps", "mbps", " mb/s", "mb/s", " mbit/s"} {
			s = strings.TrimSuffix(s, suffix)
		}
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n <= 0 {
			return 10
		}
		return n
	}
	return 10
}

func clashWsPath(opts *ClashWSOpts) string {
	if opts == nil || opts.Path == "" {
		return "/"
	}
	return opts.Path
}

func clashWsHost(opts *ClashWSOpts) string {
	if opts == nil {
		return ""
	}
	if h := opts.Headers["Host"]; h != "" {
		return h
	}
	if h := opts.Headers["host"]; h != "" {
		return h
	}
	return ""
}

func clashGRPCService(opts *ClashGRPCOpts) string {
	if opts == nil {
		return ""
	}
	return opts.ServiceName
}

func clashSNI(p ClashProxy) string {
	if p.SNI != "" {
		return p.SNI
	}
	if p.SniAlt != "" {
		return p.SniAlt
	}
	return p.Server
}

func clashFingerprint(p ClashProxy) string {
	if p.Fingerprint != "" {
		return p.Fingerprint
	}
	return p.FingerprintAlt
}

func clashTransportParams(p ClashProxy, q url.Values) {
	network := strings.ToLower(p.Network)
	if network == "" {
		network = "tcp"
	}
	q.Set("type", network)

	switch network {
	case "ws":
		q.Set("path", clashWsPath(p.WSOpts))
		if h := clashWsHost(p.WSOpts); h != "" {
			q.Set("host", h)
		}
	case "grpc":
		if svc := clashGRPCService(p.GRPCOpts); svc != "" {
			q.Set("serviceName", svc)
			q.Set("path", svc)
		}
	case "h2", "http":
		if p.H2Opts != nil {
			if len(p.H2Opts.Path) > 0 {
				q.Set("path", p.H2Opts.Path[0])
			}
			if len(p.H2Opts.Host) > 0 {
				q.Set("host", p.H2Opts.Host[0])
			}
		}
	case "httpupgrade":
		if p.HTTPUpgradeOpts != nil {
			if p.HTTPUpgradeOpts.Path != "" {
				q.Set("path", p.HTTPUpgradeOpts.Path)
			}
			if p.HTTPUpgradeOpts.Host != "" {
				q.Set("host", p.HTTPUpgradeOpts.Host)
			}
		}
	case "splithttp":
		if p.SplitHTTPOpts != nil {
			if p.SplitHTTPOpts.Path != "" {
				q.Set("path", p.SplitHTTPOpts.Path)
			}
			if p.SplitHTTPOpts.Host != "" {
				q.Set("host", p.SplitHTTPOpts.Host)
			}
		}
	}
}

func clashVMessToURI(p ClashProxy) string {
	if p.Server == "" || p.UUID == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)

	alterId := 0
	if p.AlterID != nil {
		switch x := p.AlterID.(type) {
		case int:
			alterId = x
		case float64:
			alterId = int(x)
		case string:
			alterId, _ = strconv.Atoi(x)
		}
	}

	cipher := p.Cipher
	if cipher == "" {
		cipher = "auto"
	}

	network := strings.ToLower(p.Network)
	if network == "" {
		network = "tcp"
	}

	tlsVal := ""
	if p.TLS {
		tlsVal = "tls"
	}

	sni := clashSNI(p)

	path := "/"
	host := ""
	grpcService := ""

	switch network {
	case "ws":
		path = clashWsPath(p.WSOpts)
		host = clashWsHost(p.WSOpts)
	case "grpc":
		grpcService = clashGRPCService(p.GRPCOpts)
	case "h2", "http":
		if p.H2Opts != nil {
			if len(p.H2Opts.Path) > 0 {
				path = p.H2Opts.Path[0]
			}
			if len(p.H2Opts.Host) > 0 {
				host = p.H2Opts.Host[0]
			}
		}
	case "httpupgrade":
		if p.HTTPUpgradeOpts != nil {
			path = p.HTTPUpgradeOpts.Path
			host = p.HTTPUpgradeOpts.Host
		}
	case "splithttp":
		if p.SplitHTTPOpts != nil {
			path = p.SplitHTTPOpts.Path
			host = p.SplitHTTPOpts.Host
		}
	}
	if path == "" {
		path = "/"
	}

	d := map[string]interface{}{
		"v":           "2",
		"ps":          p.Name,
		"add":         p.Server,
		"port":        portStr,
		"id":          p.UUID,
		"aid":         alterId,
		"scy":         cipher,
		"net":         network,
		"tls":         tlsVal,
		"sni":         sni,
		"host":        host,
		"path":        path,
		"serviceName": grpcService,
	}
	data, err := json.Marshal(d)
	if err != nil {
		return ""
	}
	return "vmess://" + base64.StdEncoding.EncodeToString(data)
}

func clashVLessToURI(p ClashProxy) string {
	if p.Server == "" || p.UUID == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)

	security := "none"
	if p.RealityOpts != nil {
		security = "reality"
	} else if p.TLS {
		security = "tls"
	}

	sni := clashSNI(p)

	q := url.Values{}
	clashTransportParams(p, q)
	q.Set("security", security)

	if security != "none" {
		q.Set("sni", sni)
		if fp := clashFingerprint(p); fp != "" {
			q.Set("fp", fp)
		}
		if len(p.ALPN) > 0 {
			q.Set("alpn", strings.Join(p.ALPN, ","))
		}
	}
	if security == "reality" && p.RealityOpts != nil {
		q.Set("pbk", p.RealityOpts.PublicKey)
		q.Set("sid", p.RealityOpts.ShortID)
	}
	if p.Flow != "" {
		q.Set("flow", p.Flow)
	}

	return fmt.Sprintf("vless://%s@%s:%s?%s#%s",
		url.PathEscape(p.UUID), p.Server, portStr, q.Encode(), url.PathEscape(p.Name))
}

func clashTrojanToURI(p ClashProxy) string {
	if p.Server == "" || p.Password == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)

	sni := clashSNI(p)

	q := url.Values{}
	clashTransportParams(p, q)
	q.Set("sni", sni)
	if fp := clashFingerprint(p); fp != "" {
		q.Set("fp", fp)
	}
	if len(p.ALPN) > 0 {
		q.Set("alpn", strings.Join(p.ALPN, ","))
	}

	return fmt.Sprintf("trojan://%s@%s:%s?%s#%s",
		url.PathEscape(p.Password), p.Server, portStr, q.Encode(), url.PathEscape(p.Name))
}

func clashSSToURI(p ClashProxy) string {
	if p.Server == "" || p.Cipher == "" || p.Password == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)

	userInfo := base64.StdEncoding.EncodeToString([]byte(p.Cipher + ":" + p.Password))
	q := url.Values{}

	switch p.Plugin {
	case "obfs":
		mode := ""
		host := ""
		if p.PluginOpts != nil {
			if m, ok := p.PluginOpts["mode"].(string); ok {
				mode = m
			}
			if h, ok := p.PluginOpts["host"].(string); ok {
				host = h
			}
		}
		pluginStr := "obfs-local"
		if mode != "" {
			pluginStr += ";obfs=" + mode
		}
		if host != "" {
			pluginStr += ";obfs-host=" + host
		}
		q.Set("plugin", pluginStr)
	case "v2ray-plugin":
		if p.PluginOpts != nil {
			mode, _ := p.PluginOpts["mode"].(string)
			if mode == "websocket" || mode == "" {
				pluginStr := "v2ray-plugin"
				wsPath := "/"
				if pt, ok := p.PluginOpts["path"].(string); ok && pt != "" {
					wsPath = pt
				}
				wsHost := p.Server
				if h, ok := p.PluginOpts["host"].(string); ok && h != "" {
					wsHost = h
				}
				tls, _ := p.PluginOpts["tls"].(bool)
				pluginStr += ";mode=websocket"
				pluginStr += ";path=" + wsPath
				pluginStr += ";host=" + wsHost
				if tls {
					pluginStr += ";tls"
				}
				q.Set("plugin", pluginStr)
			} else {
				return ""
			}
		}
	}

	uri := fmt.Sprintf("ss://%s@%s:%s", userInfo, p.Server, portStr)
	if len(q) > 0 {
		uri += "?" + q.Encode()
	}
	return uri + "#" + url.PathEscape(p.Name)
}

func clashSSRToURI(p ClashProxy) string {
	if p.Server == "" || p.Password == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)

	protocol := p.Protocol
	if protocol == "" {
		protocol = "origin"
	}
	cipher := p.Cipher
	if cipher == "" {
		cipher = "none"
	}
	obfs := p.Obfs
	if obfs == "" {
		obfs = "plain"
	}

	b64pass := base64.RawURLEncoding.EncodeToString([]byte(p.Password))
	body := fmt.Sprintf("%s:%s:%s:%s:%s:%s",
		p.Server, portStr, protocol, cipher, obfs, b64pass)

	b64obfsParam := base64.RawURLEncoding.EncodeToString([]byte(p.ObfsParam))
	b64protoParam := base64.RawURLEncoding.EncodeToString([]byte(p.ProtocolParam))
	b64name := base64.RawURLEncoding.EncodeToString([]byte(p.Name))
	params := fmt.Sprintf("obfsparam=%s&protoparam=%s&remarks=%s",
		b64obfsParam, b64protoParam, b64name)

	full := body + "/?" + params
	return "ssr://" + base64.RawURLEncoding.EncodeToString([]byte(full))
}

func clashHy2ToURI(p ClashProxy) string {
	if p.Server == "" || p.Password == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)
	sni := clashSNI(p)

	q := url.Values{}
	q.Set("sni", sni)
	if len(p.ALPN) > 0 {
		q.Set("alpn", strings.Join(p.ALPN, ","))
	}
	if p.Obfs == "salamander" && p.ObfsPassword != "" {
		q.Set("obfs", "salamander")
		q.Set("obfs-password", p.ObfsPassword)
	} else if p.ObfsPassword != "" {
		q.Set("obfs", "salamander")
		q.Set("obfs-password", p.ObfsPassword)
	}

	return fmt.Sprintf("hy2://%s@%s:%s?%s#%s",
		url.PathEscape(p.Password), p.Server, portStr, q.Encode(), url.PathEscape(p.Name))
}

func clashHyToURI(p ClashProxy) string {
	if p.Server == "" {
		return ""
	}
	auth := p.AuthStr
	if auth == "" {
		auth = p.AuthStrAlt
	}
	if auth == "" {
		auth = p.Auth
	}
	if auth == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)
	sni := clashSNI(p)
	up := clashBandwidthMbps(p.Up)
	down := clashBandwidthMbps(p.Down)

	q := url.Values{}
	q.Set("peer", sni)
	q.Set("sni", sni)
	q.Set("upmbps", strconv.Itoa(up))
	q.Set("downmbps", strconv.Itoa(down))
	if len(p.ALPN) > 0 {
		q.Set("alpn", strings.Join(p.ALPN, ","))
	}
	if p.Obfs != "" {
		q.Set("obfs", p.Obfs)
	}
	if p.Protocol != "" {
		q.Set("protocol", p.Protocol)
	}

	return fmt.Sprintf("hy://%s@%s:%s?%s#%s",
		url.PathEscape(auth), p.Server, portStr, q.Encode(), url.PathEscape(p.Name))
}

func clashTUICToURI(p ClashProxy) string {
	if p.Server == "" || p.UUID == "" {
		return ""
	}
	portStr := clashPortStr(p.Port)

	password := p.Password
	if password == "" {
		password = p.Token
	}
	sni := clashSNI(p)

	q := url.Values{}
	q.Set("sni", sni)
	if len(p.ALPN) > 0 {
		q.Set("alpn", strings.Join(p.ALPN, ","))
	}
	if congestion, ok := p.PluginOpts["congestion-controller"].(string); ok && congestion != "" {
		q.Set("congestion_control", congestion)
	}

	return fmt.Sprintf("tuic://%s:%s@%s:%s?%s#%s",
		url.PathEscape(p.UUID), url.PathEscape(password),
		p.Server, portStr, q.Encode(), url.PathEscape(p.Name))
}

func clashProxyToURI(p ClashProxy) string {
	ptype := strings.ToLower(strings.TrimSpace(p.Type))
	switch ptype {
	case "vmess":
		return clashVMessToURI(p)
	case "vless":
		return clashVLessToURI(p)
	case "trojan":
		return clashTrojanToURI(p)
	case "ss", "shadowsocks":
		return clashSSToURI(p)
	case "ssr", "shadowsocksr":
		return clashSSRToURI(p)
	case "hy2", "hysteria2":
		return clashHy2ToURI(p)
	case "hy", "hysteria":
		return clashHyToURI(p)
	case "tuic":
		return clashTUICToURI(p)
	}
	return ""
}

func isClashYAML(content string) bool {
	limit := len(content)
	if limit > 8192 {
		limit = 8192
	}
	head := content[:limit]
	for _, line := range strings.Split(head, "\n") {
		t := strings.TrimSpace(line)
		switch t {
		case "proxies:", "Proxies:", "proxy:", "Proxy:":
			return true
		}
		if strings.HasPrefix(t, "proxies:") || strings.HasPrefix(t, "Proxy:") {
			return true
		}
	}
	for _, marker := range []string{
		"type: vmess", "type: vless", "type: trojan",
		"type: ss\n", "type: ss\r", "type: ssr",
		"type: hysteria2", "type: hysteria\n", "type: hysteria\r",
		"type: tuic",
	} {
		if strings.Contains(head, marker) {
			return true
		}
	}
	return false
}

func parseClashYAML(content string) []string {
	var wrapper clashConfigWrapper
	if err := yaml.Unmarshal([]byte(content), &wrapper); err != nil {
		return nil
	}

	proxies := wrapper.Proxies
	if len(proxies) == 0 {
		proxies = wrapper.ProxiesOld
	}
	if len(proxies) == 0 {
		proxies = wrapper.ProxiesP
	}
	if len(proxies) == 0 {
		return nil
	}

	var lines []string
	for _, p := range proxies {
		uri := clashProxyToURI(p)
		if uri != "" {
			lines = append(lines, uri)
		}
	}
	return lines
}

func smartDecode(content string) []string {
	trimmed := strings.TrimSpace(content)
	if isClashYAML(trimmed) {
		if lines := parseClashYAML(trimmed); len(lines) > 0 {
			return lines
		}
	}
	if hasProtoPrefix(trimmed) {
		return extractLines(trimmed)
	}
	if isLikelyBase64(trimmed) {
		if decoded, err := decodeBase64([]byte(trimmed)); err == nil {
			decoded = strings.TrimSpace(decoded)
			if isClashYAML(decoded) {
				if lines := parseClashYAML(decoded); len(lines) > 0 {
					return lines
				}
			}
			if hasProtoPrefix(decoded) {
				return extractLines(decoded)
			}
			return extractLines(decoded)
		}
	}
	lines := extractLines(trimmed)
	var result []string
	for _, line := range lines {
		lineTrimmed := strings.TrimSpace(line)
		if hasProtoPrefix(lineTrimmed) {
			result = append(result, lineTrimmed)
			continue
		}
		if isLikelyBase64(lineTrimmed) {
			if decoded, err := decodeBase64([]byte(lineTrimmed)); err == nil {
				for _, dl := range extractLines(decoded) {
					if hasProtoPrefix(dl) || dl != "" {
						result = append(result, dl)
					}
				}
				continue
			}
		}
		result = append(result, lineTrimmed)
	}
	return result
}

func fetchAllFromSubs(subURLs []string) ([]string, []string) {
	const batchSize    = 20
	const fetchTimeout = 8 * time.Second

	retryCount := cfg.Validation.FetchRetryCount
	retryDelay := time.Duration(cfg.Validation.FetchRetryDelayMs) * time.Millisecond
	if retryCount < 0 {
		retryCount = 0
	}
	if retryDelay < 0 {
		retryDelay = 0
	}

	total := len(subURLs)
	numBatches := (total + batchSize - 1) / batchSize
	fmt.Printf("📡 Fetching %d sources in %d batches (timeout=%s  retries=%d)\n",
		total, numBatches, fetchTimeout, retryCount)

	var mu sync.Mutex
	var lines []string
	var failed []string
	var okCount, failCount int

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > total {
			end = total
		}
		batch := subURLs[start:end]

		var wg sync.WaitGroup
		type batchResult struct {
			url     string
			lines   []string
			err     error
			status  int
		}
		results := make([]batchResult, len(batch))

		for i, u := range batch {
			wg.Add(1)
			go func(idx int, rawURL string) {
				defer wg.Done()
				var fr fetchResult
				for attempt := 0; attempt <= retryCount; attempt++ {
					if attempt > 0 && retryDelay > 0 {
						time.Sleep(retryDelay)
					}
					fr = fetchRaw(rawURL, fetchTimeout)
					if fr.err == nil && fr.statusCode == http.StatusOK {
						break
					}
				}
				if fr.err != nil || fr.statusCode != http.StatusOK {
					results[idx] = batchResult{url: rawURL, err: fr.err, status: fr.statusCode}
					return
				}
				extracted := smartDecode(fr.content)
				results[idx] = batchResult{url: rawURL, lines: extracted, status: fr.statusCode}
			}(i, u)
		}
		wg.Wait()

		mu.Lock()
		for _, r := range results {
			if r.err != nil || r.status != http.StatusOK {
				status := "error"
				if r.status > 0 {
					status = fmt.Sprintf("HTTP %d", r.status)
				}
				failCount++
				failed = append(failed, fmt.Sprintf("%s (%s)", r.url, status))
				if gLog != nil {
					gLog.writeLine(fmt.Sprintf("[FETCH] FAIL  %s  status=%s", r.url, status))
				}
				continue
			}
			okCount++
			if gLog != nil {
				gLog.writeLine(fmt.Sprintf("[FETCH] OK    %s  lines=%d", r.url, len(r.lines)))
			}
			lines = append(lines, r.lines...)
		}
		mu.Unlock()
	}
	fmt.Printf("  ✅ Fetch done — ok=%d  fail=%d  total_lines=%d\n", okCount, failCount, len(lines))
	return lines, failed
}

func main() {
	if err := loadSettings("settings.json"); err != nil {
		fmt.Printf("❌ Failed to load settings.json: %v\n", err)
		os.Exit(1)
	}

	if err := loadClashBase("clash_base.yaml"); err != nil {
		fmt.Printf("⚠️  clash_base.yaml: %v\n", err)
	}
	if err := loadClashBaseAdvanced("clash_base_advanced.yaml"); err != nil {
		fmt.Printf("⚠️  clash_base_advanced.yaml: %v\n", err)
	}

	var logErr error
	gLog, logErr = newLogger("logs")
	if logErr != nil {
		fmt.Printf("⚠️  Log file error: %v\n", logErr)
	}
	if gLog != nil {
		defer gLog.close()
	}

	start := time.Now()
	v := cfg.Validation
	fmt.Println("🚀 Starting V2Ray config aggregator...")
	fmt.Printf("⚙️  Workers=%d | Timeout=%.0fs | Retries=%d\n",
		v.NumWorkers, v.GlobalTimeoutSec, v.MaxRetries)

	if err := prepareOutputDirs(); err != nil {
		fmt.Printf("❌ Error creating directories: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("📡 Fetching configurations from sources...")
	var allConfigs []string
	var failedLinks []string
	subURLs := loadSubsFromFile("sub.txt")
	if len(subURLs) > 0 {
		fmt.Printf("📋 Loaded %d sources from sub.txt\n", len(subURLs))
		allConfigs, failedLinks = fetchAllFromSubs(subURLs)
	} else {
		fmt.Println("⚠️  sub.txt not found or empty — falling back to settings.json links")
		allConfigs, failedLinks = fetchAll(cfg.Base64Links, cfg.TextLinks)
	}
	fmt.Printf("📊 Total fetched: %d | Failed sources: %d\n", len(allConfigs), len(failedLinks))

	if gLog != nil {
		gLog.logStart(len(allConfigs), len(failedLinks))
	}

	fmt.Println("🔍 Validating...")
	results := validateAll(allConfigs)

	elapsed := time.Since(start).Seconds()
	fmt.Printf("\n✅ Valid configurations: %d\n", len(results))

	if gLog != nil {
		gLog.logSummary(elapsed, results, failedLinks)
	}

	writeOutputFiles(results)
	writeSummary(results, failedLinks, elapsed, len(allConfigs))
	fmt.Println("✅ Done!")
}

func prepareOutputDirs() error {
	os.RemoveAll("config")
	dirs := []string{
		"config",
		cfg.Output.ProtocolsDir,
		"config/batches/v2ray",
		"config/batches/clash",
		"config/batches/clash_advanced",
		// SNI output directories
		"config/sni",
		"config/sni/protocols",
		"config/batches/sni_v2ray",
		"config/batches/sni_clash",
		"config/batches/sni_clash_advanced",
	}
	for _, dir := range dirs {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	return nil
}

func fetchAll(base64Links, textLinks []string) ([]string, []string) {
	const batchSize    = 20
	const fetchTimeout = 5 * time.Second

	retryCount := cfg.Validation.FetchRetryCount
	retryDelay := time.Duration(cfg.Validation.FetchRetryDelayMs) * time.Millisecond
	if retryCount < 0 {
		retryCount = 0
	}

	type urlJob struct {
		url      string
		isBase64 bool
	}

	var jobs []urlJob
	for _, u := range base64Links {
		jobs = append(jobs, urlJob{u, true})
	}
	for _, u := range textLinks {
		jobs = append(jobs, urlJob{u, false})
	}

	total := len(jobs)
	numBatches := (total + batchSize - 1) / batchSize
	fmt.Printf("📡 Fetching %d sources in %d batches of %d (timeout=%s  retries=%d)\n",
		total, numBatches, batchSize, fetchTimeout, retryCount)

	var mu sync.Mutex
	var lines []string
	var failed []string
	var okCount, failCount int

	for batchIdx := 0; batchIdx < numBatches; batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > total {
			end = total
		}
		batch := jobs[start:end]

		var wg sync.WaitGroup
		results := make([]fetchResult, len(batch))

		for i, job := range batch {
			wg.Add(1)
			go func(idx int, j urlJob) {
				defer wg.Done()
				var r fetchResult
				for attempt := 0; attempt <= retryCount; attempt++ {
					if attempt > 0 && retryDelay > 0 {
						time.Sleep(retryDelay)
					}
					r = fetchRaw(j.url, fetchTimeout)
					if r.err == nil && r.statusCode == http.StatusOK {
						break
					}
				}
				if r.err == nil && r.statusCode == http.StatusOK && j.isBase64 {
					decoded, err := decodeBase64([]byte(r.content))
					if err != nil {
						r.err = err
					} else {
						r.content = decoded
					}
				}
				results[idx] = r
			}(i, job)
		}
		wg.Wait()

		mu.Lock()
		for _, r := range results {
			if r.err != nil || r.statusCode != http.StatusOK {
				status := "error"
				if r.statusCode > 0 {
					status = fmt.Sprintf("HTTP %d", r.statusCode)
				}
				failCount++
				failed = append(failed, fmt.Sprintf("%s (%s)", r.url, status))
				if gLog != nil {
					gLog.writeLine(fmt.Sprintf("[FETCH] FAIL  %s  status=%s", r.url, status))
				}
				continue
			}
			okCount++
			if gLog != nil {
				gLog.writeLine(fmt.Sprintf("[FETCH] OK    %s", r.url))
			}
			lines = append(lines, strings.Split(strings.TrimSpace(r.content), "\n")...)
		}
		mu.Unlock()
	}
	fmt.Printf("  ✅ Fetch done — ok=%d  fail=%d  total_lines=%d\n", okCount, failCount, len(lines))
	return lines, failed
}

func fetchRaw(rawURL string, timeout time.Duration) fetchResult {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
	if err != nil {
		return fetchResult{url: rawURL, err: err}
	}
	resp, err := fetchHTTPClient.Do(req)
	if err != nil {
		return fetchResult{url: rawURL, err: err}
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return fetchResult{url: rawURL, statusCode: resp.StatusCode, err: err}
	}
	return fetchResult{url: rawURL, statusCode: resp.StatusCode, content: string(body)}
}

// failDetail holds per-protocol failure reason counts.
type failDetail struct {
	mu      sync.Mutex
	reasons map[string]int
	samples map[string][]string
}

func validateAll(lines []string) []configResult {
	seen := make(map[string]bool)
	byProto := make(map[string][]string)
	duplicates := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		for _, proto := range cfg.Protocols {
			if strings.HasPrefix(line, proto+"://") {
				id := coreIdentity(line, proto)
				if !seen[id] {
					seen[id] = true
					byProto[proto] = append(byProto[proto], line)
				} else {
					duplicates++
				}
				break
			}
		}
	}

	for p, lines := range byProto {
		gInputByProto[p] = len(lines)
	}

	if gLog != nil {
		gLog.writeLine(fmt.Sprintf("[DEDUP] removed=%d duplicates", duplicates))
		total := 0
		for _, p := range cfg.ProtocolOrder {
			n := len(byProto[p])
			total += n
			if n > 0 {
				gLog.writeLine(fmt.Sprintf("[DEDUP] %-6s unique=%d", p, n))
			}
		}
		gLog.writeLine(fmt.Sprintf("[DEDUP] total unique=%d", total))
		gLog.writeLine("")
	}

	protoFails := make(map[string]*failDetail)
	for _, p := range cfg.ProtocolOrder {
		protoFails[p] = &failDetail{reasons: make(map[string]int), samples: make(map[string][]string)}
	}

	var testedCount int64
	var passedCount int64
	var failedParse int64
	var failedStart int64
	var failedConn int64
	var out []configResult

	v := cfg.Validation
	batchSize := v.NumWorkers
	if batchSize <= 0 {
		batchSize = 50
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	for _, proto := range cfg.ProtocolOrder {
		protoLines := byProto[proto]
		if len(protoLines) == 0 {
			continue
		}

		rng.Shuffle(len(protoLines), func(i, j int) {
			protoLines[i], protoLines[j] = protoLines[j], protoLines[i]
		})

		if gLog != nil {
			gLog.logProtoStart(proto, len(protoLines))
		}

		protoTotal := len(protoLines)
		totalBatches := (protoTotal + batchSize - 1) / batchSize
		protoStart := time.Now()

		fmt.Printf("\n🔵 [%s] Starting — %d configs in %d batches of %d\n",
			strings.ToUpper(proto), protoTotal, totalBatches, batchSize)

		var protoPassed int64

		for batchIdx := 0; batchIdx < totalBatches; batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > protoTotal {
				end = protoTotal
			}
			batch := protoLines[start:end]
			actualBatchSize := len(batch)

			localPorts := make(chan int, actualBatchSize)
			for i := 0; i < actualBatchSize; i++ {
				localPorts <- v.BasePort + i
			}

			bt := &batchTracker{}

			type workerResult struct {
				line string
				res  validationResult
			}
			workerResults := make([]workerResult, actualBatchSize)

			var wg sync.WaitGroup
			batchStart := time.Now()

			for i, line := range batch {
				wg.Add(1)
				go func(idx int, l string) {
					defer wg.Done()
					globalIdx := atomic.AddInt64(&testedCount, 1)
					res := validateWithTracker(l, proto, localPorts, bt)
					if gLog != nil {
						gLog.logResult(globalIdx, proto, l, res)
					}
					workerResults[idx] = workerResult{line: l, res: res}
				}(i, line)
			}

			wg.Wait()

			procsAfter := countSingboxProcs()
			occupiedAfter := checkOccupiedPorts(v.BasePort, actualBatchSize)

			bt.killAll()
			if v.ProcessKillWaitMs > 0 {
				time.Sleep(time.Duration(v.ProcessKillWaitMs) * time.Millisecond)
			}

			if procsAfter > 0 || len(occupiedAfter) > 0 {
				fmt.Printf("     ⚠️  After kill  — procs:%-3d  ports-busy:%-3d\n",
					procsAfter, len(occupiedAfter))
				if len(occupiedAfter) > 0 && len(occupiedAfter) <= 20 {
					fmt.Printf("     ⚠️  Still-busy ports: %v\n", occupiedAfter)
				}
			}

			var bPassed, bFailed, bParse, bStart, bConn int

			for _, wr := range workerResults {
				res := wr.res
				if res.passed {
					bPassed++
					atomic.AddInt64(&passedCount, 1)
					atomic.AddInt64(&protoPassed, 1)
					out = append(out, configResult{line: wr.line, proto: proto})
				} else {
					bFailed++
					reason := res.failReason
					norm := classifyFailReason(reason)
					fd := protoFails[proto]
					fd.mu.Lock()
					fd.reasons[norm]++
					if len(fd.samples[norm]) < 100 {
						fd.samples[norm] = append(fd.samples[norm], wr.line)
					}
					fd.mu.Unlock()

					if strings.HasPrefix(reason, "PARSE:") {
						bParse++
						atomic.AddInt64(&failedParse, 1)
					} else if strings.HasPrefix(reason, "SINGBOX_START:") || strings.HasPrefix(reason, "START:") {
						bStart++
						atomic.AddInt64(&failedStart, 1)
					} else {
						bConn++
						atomic.AddInt64(&failedConn, 1)
					}
				}
			}

			batchElapsed := time.Since(batchStart).Seconds()
			batchPassRate := 0.0
			if actualBatchSize > 0 {
				batchPassRate = float64(bPassed) / float64(actualBatchSize) * 100
			}
			totalDone := (batchIdx + 1) * batchSize
			if totalDone > protoTotal {
				totalDone = protoTotal
			}

			fmt.Printf("  📦 Batch %d/%d [%d configs]  ✅%d ❌%d  Rate:%.1f%%  Time:%.1fs\n",
				batchIdx+1, totalBatches, actualBatchSize, bPassed, bFailed, batchPassRate, batchElapsed)
			fmt.Printf("     Parse✗:%-5d  Start✗:%-5d  Conn✗:%-5d  Total:%d/%d\n",
				bParse, bStart, bConn, totalDone, protoTotal)

			if batchIdx < totalBatches-1 && v.BatchRestMs > 0 {
				fmt.Printf("     💤 %dms rest...\n", v.BatchRestMs)
				time.Sleep(time.Duration(v.BatchRestMs) * time.Millisecond)
			}
		}

		protoElapsed := time.Since(protoStart).Seconds()
		protoPassRate := float64(protoPassed) / float64(protoTotal) * 100
		fmt.Printf("✅ [%s] Done — passed=%d/%d (%.1f%%) in %.1fs\n",
			strings.ToUpper(proto), protoPassed, protoTotal, protoPassRate, protoElapsed)
	}

	fmt.Printf("\n📊 Tested=%d | Passed=%d | ParseFail=%d | StartFail=%d | ConnFail=%d\n",
		atomic.LoadInt64(&testedCount),
		atomic.LoadInt64(&passedCount),
		atomic.LoadInt64(&failedParse),
		atomic.LoadInt64(&failedStart),
		atomic.LoadInt64(&failedConn))

	printFailureReport(protoFails, byProto)

	return out
}

func classifyFailReason(reason string) string {
	stripANSI := func(s string) string {
		return strings.Map(func(r rune) rune {
			if r == 0x1b {
				return -1
			}
			return r
		}, s)
	}
	r := stripANSI(reason)

	switch {
	case strings.HasPrefix(r, "PARSE: base64:"):
		return "PARSE › base64 decode error"
	case strings.HasPrefix(r, "PARSE: json:"):
		return "PARSE › json decode error"
	case strings.HasPrefix(r, "PARSE: url parse:"):
		return "PARSE › url parse error"
	case strings.HasPrefix(r, "PARSE: unsupported cipher:"):
		return "PARSE › unsupported SS cipher"
	case strings.HasPrefix(r, "PARSE: unsupported transport:"):
		msg := strings.TrimPrefix(r, "PARSE: unsupported transport: ")
		switch msg {
		case "xhttp", "splithttp":
			return "PARSE › unsupported transport (xhttp/splithttp)"
		default:
			return "PARSE › unsupported transport (kcp/quic/mkcp)"
		}
	case r == "PARSE: missing @" || r == "PARSE: missing server" ||
		r == "PARSE: missing uuid" || r == "PARSE: missing password" ||
		r == "PARSE: missing port" || r == "PARSE: missing auth":
		return "PARSE › " + strings.TrimPrefix(r, "PARSE: ")
	case strings.HasPrefix(r, "PARSE: port:"):
		return "PARSE › invalid port value"
	case strings.HasPrefix(r, "PARSE: reality:"):
		return "PARSE › reality missing public key"
	case strings.HasPrefix(r, "PARSE: unknown security:"):
		return "PARSE › unknown security type"
	case strings.HasPrefix(r, "PARSE:"):
		msg := strings.TrimPrefix(r, "PARSE: ")
		if len(msg) > 48 {
			msg = msg[:48] + "…"
		}
		return "PARSE › " + msg

	case strings.HasPrefix(r, "SINGBOX_START:"), strings.HasPrefix(r, "START:"):
		body := r
		if i := strings.Index(body, ": "); i != -1 {
			body = body[i+2:]
		}
		switch {
		case strings.Contains(body, "port not open"):
			return "START › port timeout (sing-box didn't listen)"
		case strings.Contains(body, "decode config"), strings.Contains(body, "outbound"):
			if strings.Contains(body, "flow") {
				return "START › invalid flow (requires TLS)"
			}
			return "START › invalid config JSON (sing-box rejected)"
		case strings.Contains(body, "address already in use"):
			return "START › port already in use"
		case strings.Contains(body, "no such file"), strings.Contains(body, "not found"):
			return "START › sing-box binary not found"
		case strings.Contains(body, "permission denied"):
			return "START › permission denied"
		case strings.Contains(body, "method"):
			return "START › unsupported SS method"
		default:
			if len(body) > 55 {
				body = body[:55] + "…"
			}
			return "START › " + body
		}

	case strings.HasPrefix(r, "CONN:"):
		body := strings.TrimPrefix(r, "CONN: ")
		if i := strings.Index(body, " | SINGBOX:"); i != -1 {
			body = body[:i]
		}
		if strings.HasPrefix(body, "Get ") {
			real := body
			if i := strings.Index(body, `": `); i != -1 {
				real = body[i+3:]
			} else if i := strings.LastIndex(body, ": "); i != -1 && i > 10 {
				real = body[i+2:]
			}
			body = real
		}
		switch {
		case strings.Contains(body, "context deadline exceeded"), strings.Contains(body, "context canceled"):
			return "CONN › request timed out (no response from proxy)"
		case strings.Contains(body, "connection refused"):
			return "CONN › connection refused (proxy died)"
		case body == "EOF" || strings.HasSuffix(body, ": EOF") || body == "unexpected EOF":
			return "CONN › EOF (proxy closed connection)"
		case strings.Contains(body, "EOF"):
			return "CONN › EOF (proxy closed connection)"
		case strings.Contains(body, "no such host"), strings.Contains(body, "lookup"):
			return "CONN › DNS resolution failed"
		case strings.Contains(body, "i/o timeout"):
			return "CONN › i/o timeout"
		case strings.Contains(body, "connection reset"):
			return "CONN › connection reset by peer"
		case strings.Contains(body, "no route to host"):
			return "CONN › no route to host"
		case strings.Contains(body, "network unreachable"):
			return "CONN › network unreachable"
		case strings.Contains(body, "tls:"), strings.Contains(body, "TLS"), strings.Contains(body, "certificate"):
			return "CONN › TLS handshake failed"
		case body == "HTTP_502":
			return "CONN › HTTP 502 (proxy rejected CONNECT)"
		case body == "HTTP_501":
			return "CONN › HTTP 501 (no CONNECT support)"
		case strings.Contains(body, "HTTP_"):
			return "CONN › unexpected HTTP status: " + body
		case strings.Contains(body, "proxyconnect"):
			return "CONN › proxy CONNECT failed"
		case strings.Contains(body, "context expired"):
			return "CONN › test URL timed out (proxy dead or unreachable)"
		default:
			if len(body) > 55 {
				body = body[:55] + "…"
			}
			return "CONN › " + body
		}

	case strings.HasPrefix(r, "FILE:"):
		return "OTHER › temp file error"
	default:
		if len(r) > 55 {
			r = r[:55] + "…"
		}
		return "OTHER › " + r
	}
}

func printFailureReport(protoFails map[string]*failDetail, byProto map[string][]string) {
	type kv struct {
		key string
		val int
	}

	const W = 78

	hr := func(ch string) { fmt.Println(strings.Repeat(ch, W)) }

	fmt.Println()
	hr("═")
	title := "  FAILURE ANALYSIS REPORT"
	fmt.Printf("%-*s%s\n", W-len(title)-1, title, "")
	fmt.Printf("  %-*s\n", W-3, "Detailed breakdown of why each config failed, grouped by root cause.")
	hr("═")

	type protoRow struct {
		name      string
		total     int
		passed    int
		parseFail int
		startFail int
		connFail  int
		otherFail int
	}
	var rows []protoRow

	for _, proto := range cfg.ProtocolOrder {
		fd := protoFails[proto]
		if fd == nil {
			continue
		}
		total := len(byProto[proto])
		if total == 0 {
			continue
		}

		var pf, sf, cf, of int
		for key, cnt := range fd.reasons {
			switch {
			case strings.HasPrefix(key, "PARSE"):
				pf += cnt
			case strings.HasPrefix(key, "START"):
				sf += cnt
			case strings.HasPrefix(key, "CONN"):
				cf += cnt
			default:
				of += cnt
			}
		}
		totalFail := pf + sf + cf + of
		rows = append(rows, protoRow{proto, total, total - totalFail, pf, sf, cf, of})
	}

	fmt.Println()
	fmt.Printf("  %-7s %7s %7s %6s  %9s %9s %9s %8s  %s\n",
		"PROTO", "TOTAL", "PASSED", "PASS%", "PARSE✗", "START✗", "CONN✗", "OTHER✗", "PASS-RATE BAR")
	fmt.Println("  " + strings.Repeat("─", W-2))
	for _, row := range rows {
		passRate := float64(row.passed) / float64(row.total) * 100
		barLen := int(passRate / 5)
		bar := strings.Repeat("▓", barLen) + strings.Repeat("░", 20-barLen)
		fmt.Printf("  %-7s %7d %7d %5.1f%%  %9d %9d %9d %8d  %s\n",
			strings.ToUpper(row.name),
			row.total, row.passed, passRate,
			row.parseFail, row.startFail, row.connFail, row.otherFail,
			bar)
	}
	fmt.Println()

	for _, proto := range cfg.ProtocolOrder {
		fd := protoFails[proto]
		if fd == nil {
			continue
		}
		total := len(byProto[proto])
		if total == 0 {
			continue
		}

		totalFails := 0
		for _, c := range fd.reasons {
			totalFails += c
		}
		passed := total - totalFails
		passRate := float64(passed) / float64(total) * 100

		fmt.Printf("┌─ %-6s ─────────────────────────────────────────────────────────────\n",
			strings.ToUpper(proto))
		fmt.Printf("│  Total: %-6d  Passed: %-6d  Failed: %-6d  Pass rate: %.1f%%\n",
			total, passed, totalFails, passRate)

		if totalFails == 0 {
			fmt.Println("│  ✓ No failures recorded.")
			fmt.Println("└" + strings.Repeat("─", W-1))
			continue
		}

		sections := []struct{ prefix, label string }{
			{"PARSE", "Parse Failures  (config could not be decoded/interpreted)"},
			{"START", "Start Failures  (sing-box refused or couldn't start)"},
			{"CONN", "Conn Failures   (proxy started but connection failed)"},
			{"OTHER", "Other / Unknown"},
		}

		for _, sec := range sections {
			var items []kv
			secTotal := 0
			for k, v := range fd.reasons {
				if strings.HasPrefix(k, sec.prefix) {
					items = append(items, kv{k, v})
					secTotal += v
				}
			}
			if len(items) == 0 {
				continue
			}
			sort.Slice(items, func(i, j int) bool { return items[i].val > items[j].val })

			secPct := float64(secTotal) / float64(totalFails) * 100
			fmt.Printf("│\n│  ▶ %s\n", sec.label)
			fmt.Printf("│    Sub-total: %d configs (%.1f%% of all failures)\n", secTotal, secPct)
			fmt.Printf("│    %-52s %7s  %6s  %s\n", "Reason", "Count", "of-sec", "Bar")
			fmt.Printf("│    %s\n", strings.Repeat("·", 72))

			for _, item := range items {
				pct := float64(item.val) / float64(secTotal) * 100
				barLen := int(pct / 5)
				if barLen > 20 {
					barLen = 20
				}
				bar := strings.Repeat("█", barLen)

				displayKey := item.key
				if i := strings.Index(displayKey, " › "); i != -1 {
					displayKey = displayKey[i+3:]
				}
				if len(displayKey) > 51 {
					displayKey = displayKey[:51] + "…"
				}

				fmt.Printf("│    %-52s %7d  %5.1f%%  %s\n",
					displayKey, item.val, pct, bar)

				if samples := fd.samples[item.key]; len(samples) > 0 {
					fmt.Printf("│    ┌─ SAMPLE CONFIGS (%d) ──────────────────────────────────────────\n", len(samples))
					for i, s := range samples {
						if len(s) > 140 {
							s = s[:140] + "…"
						}
						fmt.Printf("│    │ [%3d] %s\n", i+1, s)
					}
					fmt.Printf("│    └──────────────────────────────────────────────────────────────\n")
				}
			}
		}

		fmt.Println("└" + strings.Repeat("─", W-1))
	}

	var grandTotal, grandPassed, grandFail int
	for _, row := range rows {
		grandTotal += row.total
		grandPassed += row.passed
		grandFail += row.total - row.passed
	}
	fmt.Println()
	hr("═")
	fmt.Printf("  OVERALL  Total=%-7d  Passed=%-7d  Failed=%-7d  Pass rate=%.1f%%\n",
		grandTotal, grandPassed, grandFail,
		float64(grandPassed)/float64(grandTotal)*100)
	hr("═")
	fmt.Println()
}

func validateWithTracker(configURL, protocol string, localPorts chan int, bt *batchTracker) validationResult {
	var result validationResult

	outboundJSON, parseErr := toSingBoxOutbound(configURL, protocol)
	if parseErr != "" {
		result.failReason = "PARSE: " + parseErr
		return result
	}

	port := <-localPorts
	defer func() { localPorts <- port }()

	v := cfg.Validation
	fullConfig := buildSingBoxConfig(outboundJSON, port)

	configFile, err := os.CreateTemp("", "sb-*.json")
	if err != nil {
		result.failReason = "FILE: " + err.Error()
		return result
	}
	configPath := configFile.Name()
	configFile.Close()

	if err := os.WriteFile(configPath, []byte(fullConfig), 0644); err != nil {
		os.Remove(configPath)
		result.failReason = "FILE: " + err.Error()
		return result
	}
	defer os.Remove(configPath)

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(float64(time.Second)*(v.GlobalTimeoutSec+2)))
	defer cancel()

	var stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, singBoxPath(), "run", "-c", configPath)
	cmd.Stderr = &stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}

	if err := cmd.Start(); err != nil {
		result.failReason = "START: " + err.Error()
		return result
	}

	bt.register(cmd)

	addr := fmt.Sprintf("127.0.0.1:%d", port)
	started := waitForPort(addr,
		time.Duration(v.SingboxStartTimeoutMs)*time.Millisecond,
		time.Duration(v.SingboxStartIntervalMs)*time.Millisecond,
		time.Duration(v.PortCheckTimeoutMs)*time.Millisecond,
	)

	if !started {
		killGroup(cmd)
		sbErr := extractErrVerbose(stderr.String())
		if sbErr == "" {
			sbErr = fmt.Sprintf("port not open after %dms", v.SingboxStartTimeoutMs)
		}
		result.failReason = "SINGBOX_START: " + sbErr
		return result
	}

	proxyURL, _ := url.Parse("http://" + addr)
	client := &http.Client{
		Timeout: time.Duration(v.HTTPRequestTimeoutMs) * time.Millisecond,
		Transport: &http.Transport{
			Proxy: http.ProxyURL(proxyURL),
			DialContext: (&net.Dialer{
				Timeout:   time.Duration(v.HTTPDialTimeoutMs) * time.Millisecond,
				KeepAlive: 0,
			}).DialContext,
			MaxIdleConns:          1,
			MaxIdleConnsPerHost:   1,
			DisableKeepAlives:     true,
			ResponseHeaderTimeout: time.Duration(v.HTTPResponseTimeoutMs) * time.Millisecond,
		},
	}

	success, latency, httpErr := tryHTTP(ctx, client, v.TestURLs, v.MaxRetries)
	killGroup(cmd)

	if success {
		result.passed = true
		result.latency = latency
	} else {
		sbErr := extractErrVerbose(stderr.String())
		if sbErr != "" {
			result.failReason = "CONN: " + httpErr + " | SB:" + sbErr
		} else {
			result.failReason = "CONN: " + httpErr
		}
	}
	return result
}

func waitForPort(addr string, maxWait, interval, dialTimeout time.Duration) bool {
	elapsed := time.Duration(0)
	for elapsed < maxWait {
		time.Sleep(interval)
		elapsed += interval
		conn, err := net.DialTimeout("tcp", addr, dialTimeout)
		if err == nil {
			conn.Close()
			return true
		}
	}
	return false
}

func tryHTTP(ctx context.Context, client *http.Client, testURLs []string, maxRetries int) (bool, time.Duration, string) {
	effectiveURLs := make([]string, 0, len(testURLs))
	seen := make(map[string]bool)
	for _, u := range testURLs {
		if strings.HasPrefix(u, "http://") {
			u = "https://" + u[len("http://"):]
		}
		if !seen[u] {
			effectiveURLs = append(effectiveURLs, u)
			seen[u] = true
		}
	}
	var lastErr string
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if ctx.Err() != nil {
			return false, 0, "context expired"
		}
		for _, testURL := range effectiveURLs {
			if ctx.Err() != nil {
				return false, 0, "context expired"
			}
			start := time.Now()
			req, err := http.NewRequestWithContext(ctx, "GET", testURL, nil)
			if err != nil {
				lastErr = err.Error()
				continue
			}
			resp, err := client.Do(req)
			if err != nil {
				e := shortenErr(err.Error())
				lastErr = e
				if strings.Contains(e, "connection refused") ||
					strings.Contains(e, "connection reset") ||
					strings.Contains(e, "no route to host") ||
					strings.Contains(e, "network unreachable") {
					return false, 0, lastErr
				}
				continue
			}
			latency := time.Since(start)
			code := resp.StatusCode
			resp.Body.Close()

			if code == 200 || code == 204 {
				return true, latency, ""
			}
			if code == 301 || code == 302 || code == 307 || code == 308 {
				return true, latency, ""
			}
			if code == 400 || code == 403 || code == 404 || code == 429 {
				return true, latency, ""
			}
			lastErr = fmt.Sprintf("HTTP_%d", code)
		}
	}
	return false, 0, lastErr
}

func buildSingBoxConfig(outboundJSON string, port int) string {
	return fmt.Sprintf(`{"log":{"level":"error","timestamp":false},"dns":{"servers":[{"tag":"dns-remote","address":"https://8.8.8.8/dns-query","address_resolver":"dns-direct","strategy":"prefer_ipv4","detour":"proxy"},{"tag":"dns-direct","address":"8.8.8.8","strategy":"prefer_ipv4","detour":"direct"}],"rules":[{"outbound":"any","server":"dns-direct"}],"independent_cache":true},"inbounds":[{"type":"http","tag":"http-in","listen":"127.0.0.1","listen_port":%d}],"outbounds":[%s,{"type":"direct","tag":"direct"},{"type":"block","tag":"block"}]}`,
		port, outboundJSON)
}

func toSingBoxOutbound(configURL, protocol string) (string, string) {
	switch protocol {
	case "vmess":
		return parseVMess(configURL)
	case "vless":
		return parseVLess(configURL)
	case "trojan":
		return parseTrojan(configURL)
	case "ss":
		return parseShadowsocks(configURL)
	case "hy2":
		return parseHysteria2(configURL)
	case "hy":
		return parseHysteria(configURL)
	case "tuic":
		return parseTUIC(configURL)
	case "ssr":
		return parseSSR(configURL)
	}
	return "", "unsupported protocol: " + protocol
}

// sanitizeProxyURL cleans HTML entities, control chars, and resolves
// recursively percent-encoded sequences (e.g. %25252525...XX → actual char).
// This handles configs from Telegram/HTML sources that have been double- or
// triple-encoded, such as UUIDs containing %252525...F0%25...9F...
func sanitizeProxyURL(raw string) string {
	// ── HTML entity decode ──────────────────────────────────────────────
	raw = strings.ReplaceAll(raw, "&amp;", "&")
	raw = strings.ReplaceAll(raw, "&lt;", "<")
	raw = strings.ReplaceAll(raw, "&gt;", ">")
	raw = strings.ReplaceAll(raw, "&quot;", `"`)
	raw = strings.ReplaceAll(raw, "&#39;", "'")

	// Strip spaces and control characters
	raw = strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' || r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, raw)

	// ── Recursive percent-decode ────────────────────────────────────────
	// Split at "://" to preserve the scheme, then decode only the rest.
	// We iterate until stable (no more %25 sequences to unwrap).
	schemeIdx := strings.Index(raw, "://")
	if schemeIdx == -1 {
		return raw
	}
	scheme := raw[:schemeIdx+3]
	rest := raw[schemeIdx+3:]

	// Iteratively unescape until no more %25 remain or value stops changing
	const maxIter = 20
	for i := 0; i < maxIter; i++ {
		// Only continue decoding if there are still percent-encoded sequences
		if !strings.Contains(rest, "%") {
			break
		}
		decoded, err := url.QueryUnescape(rest)
		if err != nil {
			// If full unescape fails try PathUnescape (more lenient)
			decoded, err = url.PathUnescape(rest)
			if err != nil || decoded == rest {
				break
			}
		}
		if decoded == rest {
			break
		}
		// Safety: if after decoding we no longer have a valid host portion, stop
		// (prevents over-decoding that destroys the URL structure)
		if strings.ContainsAny(decoded, "\x00\x01\x02\x03\x04\x05\x06\x07\x08\x0b\x0c\x0e\x0f") {
			break
		}
		rest = decoded
	}

	// Re-assemble: separate fragment/query so we don't re-encode them
	frag := ""
	if fragIdx := strings.LastIndex(rest, "#"); fragIdx != -1 {
		frag = rest[fragIdx:]
		rest = rest[:fragIdx]
	}
	query := ""
	if queryIdx := strings.Index(rest, "?"); queryIdx != -1 {
		query = rest[queryIdx:]
		rest = rest[:queryIdx]
	}
	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		return scheme + rest + query + frag
	}
	return scheme + encodeUserInfo(rest[:lastAt]) + "@" + rest[lastAt+1:] + query + frag
}

func normalizeUUID(u string) string {
	if len(u) == 32 {
		allHex := true
		for _, c := range u {
			if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
				allHex = false
				break
			}
		}
		if allHex {
			return u[0:8] + "-" + u[8:12] + "-" + u[12:16] + "-" + u[16:20] + "-" + u[20:32]
		}
	}
	return u
}

func encodeUserInfo(s string) string {
	var buf strings.Builder
	for i := 0; i < len(s); i++ {
		b := s[i]
		if (b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') ||
			b == '-' || b == '.' || b == '_' || b == '~' || b == '!' || b == '$' ||
			b == '&' || b == '\'' || b == '(' || b == ')' || b == '*' || b == '+' ||
			b == ',' || b == ';' || b == '=' || b == ':' {
			buf.WriteByte(b)
		} else {
			fmt.Fprintf(&buf, "%%%02X", b)
		}
	}
	return buf.String()
}

func parseVMessURItoD(data string) (map[string]interface{}, string) {
	u, err := url.Parse("vmess://" + data)
	if err != nil {
		return nil, "uri parse: " + err.Error()
	}
	uuid := u.User.Username()
	if uuid == "" {
		return nil, "missing uuid"
	}
	host := u.Hostname()
	if host == "" {
		return nil, "missing server"
	}
	portStr := u.Port()
	if portStr == "" {
		portStr = "443"
	}
	q := u.Query()
	sec := strings.ToLower(q.Get("security"))
	tlsVal := ""
	if sec == "tls" || sec == "xtls" {
		tlsVal = "tls"
	}
	d := map[string]interface{}{
		"id": uuid, "add": host, "port": portStr,
		"aid": first(q.Get("aid"), q.Get("alterId"), "0"),
		"scy": first(q.Get("encryption"), q.Get("scy"), "auto"),
		"net": first(q.Get("type"), q.Get("net"), "tcp"),
		"tls": tlsVal,
		"sni": first(q.Get("sni"), q.Get("peer"), host),
		"path": q.Get("path"),
		"host": q.Get("host"),
		"serviceName": q.Get("serviceName"),
		"fp": q.Get("fp"),
	}
	return d, ""
}

func parseVMess(raw string) (string, string) {
	data := strings.TrimPrefix(raw, "vmess://")
	if idx := strings.LastIndex(data, "#"); idx != -1 {
		data = data[:idx]
	}
	data = strings.TrimSpace(data)

	var d map[string]interface{}

	if strings.HasPrefix(data, "{") {
		if err := json.Unmarshal([]byte(data), &d); err != nil {
			return "", "json: " + err.Error()
		}
	} else {
		var tryB64 []string
		tryB64 = append(tryB64, data)
		if lastAt := strings.LastIndex(data, "@"); lastAt > 0 {
			tryB64 = append(tryB64, data[:lastAt])
		}
		{
			clean := data
			for i, c := range data {
				if c != '+' && c != '/' && c != '=' &&
					c != '-' && c != '_' &&
					!(c >= 'A' && c <= 'Z') &&
					!(c >= 'a' && c <= 'z') &&
					!(c >= '0' && c <= '9') {
					clean = data[:i]
					break
				}
			}
			if clean != data && clean != "" {
				tryB64 = append(tryB64, clean)
			}
		}

		var parsed bool
		var b64Err error
		for _, candidate := range tryB64 {
			var decoded string
			decoded, b64Err = decodeBase64([]byte(candidate))
			if b64Err != nil {
				continue
			}
			var tmp map[string]interface{}
			if json.Unmarshal([]byte(decoded), &tmp) == nil {
				d = tmp
				parsed = true
				break
			}
		}
		if !parsed {
			atIdx := strings.Index(data, "@")
			qIdx := strings.Index(data, "?")
			if atIdx != -1 && (qIdx == -1 || atIdx < qIdx) {
				var parseErr string
				d, parseErr = parseVMessURItoD(data)
				if parseErr != "" {
					return "", parseErr
				}
			} else {
				if b64Err != nil {
					return "", "base64: " + b64Err.Error()
				}
				return "", "json: invalid vmess payload"
			}
		}
	}
	server := strings.TrimSpace(fmt.Sprintf("%v", d["add"]))
	if server == "" {
		return "", "missing server"
	}
	port, err := toPort(fmt.Sprintf("%v", d["port"]))
	if err != nil {
		return "", "port: " + err.Error()
	}
	uuid := strings.TrimSpace(fmt.Sprintf("%v", d["id"]))
	if uuid == "" {
		return "", "missing uuid"
	}
	alterId := 0
	if v, ok := d["aid"]; ok {
		switch x := v.(type) {
		case float64:
			alterId = int(x)
		case string:
			alterId, _ = strconv.Atoi(x)
		}
	}
	security := "auto"
	if s, _ := d["scy"].(string); s != "" {
		security = s
	}
	network := "tcp"
	if n, _ := d["net"].(string); n != "" {
		network = strings.ToLower(n)
	}
	switch network {
	case "xhttp", "splithttp", "kcp", "mkcp", "quic":
		return "", "unsupported transport: " + network
	}
	tls := ""
	if tlsVal, _ := d["tls"].(string); tlsVal == "tls" {
		sni := server
		if s, _ := d["sni"].(string); s != "" {
			sni = s
		} else if h, _ := d["host"].(string); h != "" {
			sni = h
		}
		tls = fmt.Sprintf(`,"tls":{"enabled":true,"insecure":true,"server_name":%q}`, sni)
	}
	return fmt.Sprintf(`{"type":"vmess","tag":"proxy","server":%q,"server_port":%d,"uuid":%q,"security":%q,"alter_id":%d%s%s}`,
		server, port, uuid, security, alterId, tls, vmessTransport(d, network)), ""
}

func vmessTransport(d map[string]interface{}, network string) string {
	path := strDefault(d["path"], "/")
	host := strDefault(d["host"], "")
	svcName := strDefault(d["serviceName"], strDefault(d["path"], ""))
	return buildTransportJSON(network, path, host, svcName)
}

var singboxSupportedFlows = map[string]bool{
	"":                         true,
	"xtls-rprx-vision":         true,
	"xtls-rprx-vision-udp443":  true,
}

func parseVLess(raw string) (string, string) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", "url parse: " + err.Error()
	}
	uuid := normalizeUUID(u.User.Username())
	if uuid == "" {
		return "", "missing uuid"
	}
	server := u.Hostname()
	if server == "" {
		return "", "missing server"
	}
	port, err := toPort(u.Port())
	if err != nil {
		return "", "port: " + err.Error()
	}
	q := u.Query()
	security := strings.TrimSpace(strings.ToLower(q.Get("security")))
	network := strings.ToLower(q.Get("type"))
	if network == "" {
		network = "tcp"
	}
	switch network {
	case "xhttp", "splithttp", "kcp", "mkcp", "quic":
		return "", "unsupported transport: " + network
	}
	sni := first(q.Get("sni"), q.Get("peer"), server)
	flow := q.Get("flow")
	if !singboxSupportedFlows[flow] {
		flow = ""
	}
	tlsJSON, tlsErr := vlessTLS(security, sni, flow, q)
	if tlsErr != "" {
		return "", tlsErr
	}
	transport := buildTransportJSON(network, first(q.Get("path"), "/"), q.Get("host"),
		first(q.Get("serviceName"), q.Get("path")))
	return fmt.Sprintf(`{"type":"vless","tag":"proxy","server":%q,"server_port":%d,"uuid":%q%s%s}`,
		server, port, uuid, tlsJSON, transport), ""
}

func vlessTLS(security, sni, flow string, q url.Values) (string, string) {
	flowJSON := ""
	if flow != "" {
		flowJSON = fmt.Sprintf(`,"flow":%q`, flow)
	}
	switch security {
	case "tls", "xtls":
		s := fmt.Sprintf(`,"tls":{"enabled":true,"insecure":true,"server_name":%q`, sni)
		if fp := q.Get("fp"); fp != "" {
			s += fmt.Sprintf(`,"utls":{"enabled":true,"fingerprint":%q}`, fp)
		}
		if alpnStr := q.Get("alpn"); alpnStr != "" {
			ab, _ := json.Marshal(strings.Split(alpnStr, ","))
			s += fmt.Sprintf(`,"alpn":%s`, ab)
		}
		return s + "}", ""
	case "reality":
		pbk := q.Get("pbk")
		if pbk == "" {
			return "", "reality: missing public key (pbk)"
		}
		return flowJSON + fmt.Sprintf(`,"tls":{"enabled":true,"server_name":%q,"utls":{"enabled":true,"fingerprint":%q},"reality":{"enabled":true,"public_key":%q,"short_id":%q}}`,
			sni, first(q.Get("fp"), "chrome"), pbk, q.Get("sid")), ""
	case "none", "":
		return "", ""
	}
	return "", "unknown security: " + security
}

func buildTransportJSON(network, path, host, grpcService string) string {
	if path == "" {
		path = "/"
	}
	switch network {
	case "ws":
		if host != "" {
			return fmt.Sprintf(`,"transport":{"type":"ws","path":%q,"headers":{"Host":%q}}`, path, host)
		}
		return fmt.Sprintf(`,"transport":{"type":"ws","path":%q}`, path)
	case "grpc":
		return fmt.Sprintf(`,"transport":{"type":"grpc","service_name":%q}`, grpcService)
	case "h2", "http":
		if host != "" {
			return fmt.Sprintf(`,"transport":{"type":"http","host":[%q],"path":%q}`, host, path)
		}
		return fmt.Sprintf(`,"transport":{"type":"http","path":%q}`, path)
	case "tcp":
		return ""
	case "httpupgrade":
		if host != "" {
			return fmt.Sprintf(`,"transport":{"type":"httpupgrade","path":%q,"host":%q}`, path, host)
		}
		return fmt.Sprintf(`,"transport":{"type":"httpupgrade","path":%q}`, path)
	case "splithttp", "xhttp":
		if host != "" {
			return fmt.Sprintf(`,"transport":{"type":"splithttp","path":%q,"host":%q}`, path, host)
		}
		return fmt.Sprintf(`,"transport":{"type":"splithttp","path":%q}`, path)
	}
	return ""
}

func parseTrojan(raw string) (string, string) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", "url parse: " + err.Error()
	}
	password := u.User.Username()
	if password == "" {
		return "", "missing password"
	}
	server := u.Hostname()
	if server == "" {
		return "", "missing server"
	}
	port, err := toPort(u.Port())
	if err != nil {
		return "", "port: " + err.Error()
	}
	q := u.Query()
	sni := first(q.Get("sni"), q.Get("peer"), server)
	tls := fmt.Sprintf(`,"tls":{"enabled":true,"insecure":true,"server_name":%q`, sni)
	if fp := q.Get("fp"); fp != "" {
		tls += fmt.Sprintf(`,"utls":{"enabled":true,"fingerprint":%q}`, fp)
	}
	tls += "}"
	network := strings.ToLower(q.Get("type"))
	switch network {
	case "xhttp", "splithttp", "kcp", "mkcp", "quic":
		return "", "unsupported transport: " + network
	}
	transport := buildTransportJSON(network, first(q.Get("path"), "/"), q.Get("host"),
		first(q.Get("serviceName"), q.Get("path")))
	return fmt.Sprintf(`{"type":"trojan","tag":"proxy","server":%q,"server_port":%d,"password":%q%s%s}`,
		server, port, password, tls, transport), ""
}

var singboxSupportedSSCiphers = map[string]bool{
	"aes-128-gcm": true, "aes-192-gcm": true, "aes-256-gcm": true,
	"aes-128-cfb": true, "aes-192-cfb": true, "aes-256-cfb": true,
	"aes-128-ctr": true, "aes-192-ctr": true, "aes-256-ctr": true,
	"chacha20-ietf-poly1305": true, "xchacha20-ietf-poly1305": true,
	"chacha20-ietf": true,
	"2022-blake3-aes-128-gcm":       true,
	"2022-blake3-aes-256-gcm":       true,
	"2022-blake3-chacha20-poly1305": true,
	"none": true, "plain": true,
}

func parseShadowsocks(raw string) (string, string) {
	trimmed := strings.TrimPrefix(raw, "ss://")
	if idx := strings.LastIndex(trimmed, "#"); idx != -1 {
		trimmed = trimmed[:idx]
	}
	trimmed = strings.TrimSpace(trimmed)

	var method, password, server string
	var port int

	fastPathOK := false
	if fastU, err := url.Parse("ss://" + trimmed); err == nil &&
		fastU.User != nil && fastU.Hostname() != "" {
		uname := fastU.User.Username()
		pwd, hasPwd := fastU.User.Password()
		host := fastU.Hostname()
		portStr := fastU.Port()
		if portStr == "" {
			portStr = "443"
		}
		var m, p string
		if hasPwd {
			m, p = uname, pwd
		} else {
			if d, derr := decodeBase64([]byte(uname)); derr == nil && strings.Contains(d, ":") {
				parts := strings.SplitN(d, ":", 2)
				m, p = parts[0], parts[1]
			}
		}
		if m != "" && host != "" {
			if pVal, perr := toPort(portStr); perr == nil {
				method, password, server, port = m, p, host, pVal
				fastPathOK = true
			}
		}
	}

	if !fastPathOK {
		atIdx := strings.LastIndex(trimmed, "@")
		if atIdx == -1 {
			b64Src := trimmed
			if qi := strings.Index(b64Src, "?"); qi != -1 {
				b64Src = b64Src[:qi]
			}
			decoded, err := decodeBase64([]byte(b64Src))
			if err != nil {
				decoded = trimmed
			}
			atIdx2 := strings.LastIndex(decoded, "@")
			if atIdx2 == -1 {
				return "", "missing @"
			}
			userPart := decoded[:atIdx2]
			hostPart := decoded[atIdx2+1:]
			if idx := strings.Index(hostPart, "?"); idx != -1 {
				hostPart = hostPart[:idx]
			}
			m, p, s, po, e := ssParseUserAndHost(userPart, hostPart)
			if e != "" {
				return "", e
			}
			method, password, server, port = m, p, s, po
		} else {
			userPart := trimmed[:atIdx]
			hostPart := trimmed[atIdx+1:]
			if idx := strings.Index(hostPart, "?"); idx != -1 {
				hostPart = hostPart[:idx]
			}
			m, p, s, po, e := ssParseUserAndHost(userPart, hostPart)
			if e != "" {
				return "", e
			}
			method, password, server, port = m, p, s, po
		}
	}

	method = strings.ToLower(method)
	if !singboxSupportedSSCiphers[method] {
		return "", fmt.Sprintf("unsupported cipher: %s", method)
	}
	if server == "" {
		return "", "missing server"
	}
	return fmt.Sprintf(`{"type":"shadowsocks","tag":"proxy","server":%q,"server_port":%d,"method":%q,"password":%q}`,
		server, port, method, password), ""
}

func ssParseUserAndHost(userPart, hostPart string) (method, password, server string, port int, errMsg string) {
	decodeUser := func(s string) string {
		if d, err := decodeBase64([]byte(s)); err == nil && strings.Contains(d, ":") {
			return d
		}
		if unescaped, err := url.PathUnescape(s); err == nil && unescaped != s {
			if d, err2 := decodeBase64([]byte(unescaped)); err2 == nil && strings.Contains(d, ":") {
				return d
			}
			if strings.Contains(unescaped, ":") {
				return unescaped
			}
		}
		if colonIdx := strings.Index(s, ":"); colonIdx != -1 {
			prefix := s[:colonIdx]
			suffix := s[colonIdx+1:]
			if d, err := decodeBase64([]byte(prefix)); err == nil && !strings.Contains(d, ":") {
				return d + ":" + suffix
			}
		}
		return s
	}

	decoded := decodeUser(userPart)

	parts := strings.SplitN(decoded, ":", 2)
	if len(parts) != 2 || parts[0] == "" {
		return "", "", "", 0, "invalid user info"
	}
	method = strings.TrimSpace(parts[0])
	password = parts[1]

	hostPart = strings.TrimSpace(hostPart)
	var portStr string
	if strings.HasPrefix(hostPart, "[") {
		closeBracket := strings.Index(hostPart, "]")
		if closeBracket == -1 {
			return "", "", "", 0, "invalid IPv6 host"
		}
		server = hostPart[1:closeBracket]
		rest := hostPart[closeBracket+1:]
		if strings.HasPrefix(rest, ":") {
			portStr = rest[1:]
		} else {
			portStr = "443"
		}
	} else {
		lastColon := strings.LastIndex(hostPart, ":")
		if lastColon == -1 {
			return "", "", "", 0, "missing port"
		}
		server = hostPart[:lastColon]
		portStr = hostPart[lastColon+1:]
	}

	if idx := strings.IndexFunc(portStr, func(r rune) bool { return r < '0' || r > '9' }); idx != -1 {
		portStr = portStr[:idx]
	}
	portStr = strings.TrimSpace(portStr)
	p, err := toPort(portStr)
	if err != nil {
		return "", "", "", 0, "port: " + err.Error()
	}
	return method, password, server, p, ""
}

func parseHysteria2(raw string) (string, string) {
	trimmed := strings.TrimPrefix(raw, "hy2://")
	if i := strings.LastIndex(trimmed, "#"); i != -1 {
		trimmed = trimmed[:i]
	}
	queryStr := ""
	if i := strings.Index(trimmed, "?"); i != -1 {
		queryStr = trimmed[i+1:]
		trimmed = trimmed[:i]
	}
	lastAt := strings.LastIndex(trimmed, "@")
	if lastAt == -1 {
		return "", "missing @"
	}
	password := trimmed[:lastAt]
	hostPort := trimmed[lastAt+1:]
	if password == "" {
		return "", "missing password"
	}
	if i := strings.Index(hostPort, "/"); i != -1 {
		hostPort = hostPort[:i]
	}
	lastColon := strings.LastIndex(hostPort, ":")
	var server string
	var port int
	if lastColon == -1 {
		server = hostPort
		port = 443
	} else {
		portCandidate := hostPort[lastColon+1:]
		if _, perr := toPort(portCandidate); perr == nil {
			server = hostPort[:lastColon]
			port, _ = toPort(portCandidate)
		} else if strings.HasPrefix(hostPort, "[") {
			server = hostPort
			port = 443
		} else {
			return "", "missing port"
		}
	}
	if server == "" {
		return "", "missing server"
	}
	q, _ := url.ParseQuery(queryStr)
	obfsJSON := ""
	if obfs := q.Get("obfs"); obfs == "salamander" {
		if obfsPwd := q.Get("obfs-password"); obfsPwd != "" {
			obfsJSON = fmt.Sprintf(`,"obfs":{"type":"salamander","password":%q}`, obfsPwd)
		}
	}
	return fmt.Sprintf(`{"type":"hysteria2","tag":"proxy","server":%q,"server_port":%d,"password":%q%s,"tls":{"enabled":true,"insecure":true,"server_name":%q}}`,
		server, port, password, obfsJSON, first(q.Get("sni"), server)), ""
}

func parseHysteria(raw string) (string, string) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", "url parse: " + err.Error()
	}
	server := u.Hostname()
	if server == "" {
		return "", "missing server"
	}
	port, err := toPort(u.Port())
	if err != nil {
		return "", "port: " + err.Error()
	}
	q := u.Query()
	auth := first(q.Get("auth"), u.User.Username())
	if auth == "" {
		return "", "missing auth"
	}
	up, _ := strconv.Atoi(first(q.Get("upmbps"), "10"))
	down, _ := strconv.Atoi(first(q.Get("downmbps"), "50"))
	if up <= 0 {
		up = 10
	}
	if down <= 0 {
		down = 50
	}
	obfs := q.Get("obfs")
	obfsJSON := ""
	if obfs != "" {
		obfsJSON = fmt.Sprintf(`,"obfs":%q`, obfs)
	}
	return fmt.Sprintf(`{"type":"hysteria","tag":"proxy","server":%q,"server_port":%d,"up_mbps":%d,"down_mbps":%d,"auth_str":%q%s,"tls":{"enabled":true,"insecure":true,"server_name":%q}}`,
		server, port, up, down, auth, obfsJSON, first(q.Get("peer"), q.Get("sni"), server)), ""
}

func parseTUIC(raw string) (string, string) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", "url parse: " + err.Error()
	}
	uuid := u.User.Username()
	if uuid == "" {
		return "", "missing uuid"
	}
	password, _ := u.User.Password()
	server := u.Hostname()
	if server == "" {
		return "", "missing server"
	}
	port, err := toPort(u.Port())
	if err != nil {
		return "", "port: " + err.Error()
	}
	q := u.Query()
	sni := first(q.Get("sni"), server)
	congestion := first(q.Get("congestion_control"), q.Get("congestion-controller"), "bbr")
	udpRelayMode := q.Get("udp_relay_mode")
	udpJSON := ""
	if udpRelayMode != "" {
		udpJSON = fmt.Sprintf(`,"udp_relay_mode":%q`, udpRelayMode)
	}
	return fmt.Sprintf(`{"type":"tuic","tag":"proxy","server":%q,"server_port":%d,"uuid":%q,"password":%q,"congestion_control":%q%s,"tls":{"enabled":true,"insecure":true,"server_name":%q}}`,
		server, port, uuid, password, congestion, udpJSON, sni), ""
}

func parseSSR(raw string) (string, string) {
	trimmed := strings.TrimPrefix(raw, "ssr://")
	if trimmed == "" {
		return "", "empty ssr url"
	}
	decoded, err := decodeBase64([]byte(trimmed))
	if err != nil {
		return "", "base64: " + err.Error()
	}
	params := ""
	if i := strings.Index(decoded, "/?"); i != -1 {
		params = decoded[i+2:]
		decoded = decoded[:i]
	} else if i := strings.Index(decoded, "?"); i != -1 {
		params = decoded[i+1:]
		decoded = decoded[:i]
	}
	parts := strings.SplitN(decoded, ":", 6)
	if len(parts) < 6 {
		return "", "invalid ssr format (need host:port:protocol:method:obfs:password)"
	}
	host, portStr, protocol, method, obfs, b64pass := parts[0], parts[1], parts[2], parts[3], parts[4], parts[5]
	_ = protocol
	_ = obfs
	passDecoded, err := decodeBase64([]byte(b64pass))
	if err != nil {
		return "", "base64 password: " + err.Error()
	}
	_, err = toPort(portStr)
	if err != nil {
		return "", "port: " + err.Error()
	}
	_ = host
	_ = method
	_ = params
	_ = passDecoded
	return "", "SSR not supported by sing-box (collect-only protocol)"
}

func coreIdentity(line, protocol string) string {
	switch protocol {
	case "vmess":
		data := strings.TrimPrefix(line, "vmess://")
		if idx := strings.LastIndex(data, "#"); idx != -1 {
			data = data[:idx]
		}
		data = strings.TrimSpace(data)
		var jsonStr string
		if strings.HasPrefix(data, "{") {
			jsonStr = data
		} else {
			decoded, err := decodeBase64([]byte(data))
			if err != nil {
				return line
			}
			jsonStr = decoded
		}
		var d struct {
			Add  string      `json:"add"`
			Port interface{} `json:"port"`
			ID   string      `json:"id"`
		}
		json.Unmarshal([]byte(jsonStr), &d)
		return fmt.Sprintf("vmess://%s:%v#%s", d.Add, d.Port, d.ID)
	case "ssr":
		data := strings.TrimPrefix(line, "ssr://")
		if idx := strings.LastIndex(data, "#"); idx != -1 {
			data = data[:idx]
		}
		decoded, err := decodeBase64([]byte(strings.TrimSpace(data)))
		if err != nil {
			return line
		}
		parts := strings.SplitN(decoded, ":", 6)
		if len(parts) < 2 {
			return line
		}
		return fmt.Sprintf("ssr://%s:%s", parts[0], parts[1])
	default:
		u, err := url.Parse(sanitizeProxyURL(line))
		if err != nil || u.Hostname() == "" {
			return line
		}
		return fmt.Sprintf("%s://%s@%s:%s", protocol, u.User.String(), u.Hostname(), u.Port())
	}
}

// ── SNI Config Conversion ────────────────────────────────────────────────────
//
// toSNIConfig replaces the server address and port in a proxy URI with
// sniHost (127.0.0.1) and sniPort (40443), preserving all other parameters
// (TLS, transport, SNI, etc.) intact.
// The SNI field (server_name/sni/peer) is kept as-is so the client still
// sends the correct TLS SNI during handshake.
// Returns "" if the config cannot be transformed.
func toSNIConfig(line, proto string) string {
	switch proto {
	case "vmess":
		return toSNIVMess(line)
	case "vless", "trojan", "hy2", "hy", "tuic", "ss":
		return toSNIGeneric(line, proto)
	case "ssr":
		return toSNISSR(line)
	}
	return ""
}

// toSNIVMess handles the base64-JSON vmess format.
func toSNIVMess(line string) string {
	data := strings.TrimPrefix(line, "vmess://")
	fragSuffix := ""
	if idx := strings.LastIndex(data, "#"); idx != -1 {
		fragSuffix = data[idx:]
		data = data[:idx]
	}
	data = strings.TrimSpace(data)

	var d map[string]interface{}
	var isURI bool

	if strings.HasPrefix(data, "{") {
		if err := json.Unmarshal([]byte(data), &d); err != nil {
			return ""
		}
	} else {
		decoded, err := decodeBase64([]byte(data))
		if err == nil {
			if json.Unmarshal([]byte(decoded), &d) == nil {
				// base64 JSON path
			} else {
				d = nil
			}
		}
		if d == nil {
			if atIdx := strings.Index(data, "@"); atIdx != -1 {
				isURI = true
			}
		}
	}

	if isURI {
		// URI format: replace host:port
		atIdx := strings.LastIndex(data, "@")
		if atIdx == -1 {
			return ""
		}
		userPart := data[:atIdx]
		rest := data[atIdx+1:]
		// rest = host:port[?query]
		qIdx := strings.Index(rest, "?")
		hostPort := rest
		querySuffix := ""
		if qIdx != -1 {
			hostPort = rest[:qIdx]
			querySuffix = rest[qIdx:]
		}
		// Extract original host for SNI (keep in query if not already there)
		_ = hostPort
		return fmt.Sprintf("vmess://%s@%s:%d%s%s",
			userPart, sniHost, sniPort, querySuffix, fragSuffix)
	}

	if d == nil {
		return ""
	}

	// Replace add and port
	d["add"] = sniHost
	d["port"] = strconv.Itoa(sniPort)

	// Rebuild base64 JSON
	keys := make([]string, 0, len(d))
	for k := range d {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var buf bytes.Buffer
	buf.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			buf.WriteByte(',')
		}
		kj, _ := json.Marshal(k)
		vj, _ := json.Marshal(d[k])
		buf.Write(kj)
		buf.WriteByte(':')
		buf.Write(vj)
	}
	buf.WriteByte('}')
	return "vmess://" + base64.StdEncoding.EncodeToString(buf.Bytes()) + fragSuffix
}

// toSNIGeneric handles URI-based protocols: vless, trojan, hy2, hy, tuic, ss.
// It replaces host:port in the authority section while keeping query/fragment.
func toSNIGeneric(line, proto string) string {
	// Find scheme://
	schemeEnd := strings.Index(line, "://")
	if schemeEnd == -1 {
		return ""
	}
	scheme := line[:schemeEnd+3]
	rest := line[schemeEnd+3:]

	// Split off fragment
	frag := ""
	if idx := strings.LastIndex(rest, "#"); idx != -1 {
		frag = rest[idx:]
		rest = rest[:idx]
	}
	// Split off query
	query := ""
	if idx := strings.Index(rest, "?"); idx != -1 {
		query = rest[idx:]
		rest = rest[:idx]
	}
	// rest is now: [userinfo@]host:port[/path]
	// Split off path
	path := ""
	if idx := strings.Index(rest, "/"); idx != -1 {
		path = rest[idx:]
		rest = rest[:idx]
	}

	// Find last @ to separate userinfo from host:port
	atIdx := strings.LastIndex(rest, "@")
	var userInfo, hostPort string
	if atIdx != -1 {
		userInfo = rest[:atIdx+1] // includes "@"
		hostPort = rest[atIdx+1:]
	} else {
		hostPort = rest
	}

	// Replace host:port — handle IPv6 brackets
	newHostPort := fmt.Sprintf("%s:%d", sniHost, sniPort)
	if strings.HasPrefix(hostPort, "[") {
		// IPv6: [addr]:port — replace whole thing
		newHostPort = fmt.Sprintf("%s:%d", sniHost, sniPort)
	}
	_ = hostPort // original value not needed further

	return scheme + userInfo + newHostPort + path + query + frag
}

// toSNISSR handles the base64-encoded SSR format.
func toSNISSR(line string) string {
	trimmed := strings.TrimPrefix(line, "ssr://")
	decoded, err := decodeBase64([]byte(trimmed))
	if err != nil {
		return ""
	}
	// Format: host:port:protocol:method:obfs:b64pass[/?params]
	params := ""
	body := decoded
	if i := strings.Index(decoded, "/?"); i != -1 {
		params = decoded[i:]
		body = decoded[:i]
	} else if i := strings.Index(decoded, "?"); i != -1 {
		params = decoded[i:]
		body = decoded[:i]
	}
	parts := strings.SplitN(body, ":", 6)
	if len(parts) < 6 {
		return ""
	}
	// Replace host (parts[0]) and port (parts[1])
	parts[0] = sniHost
	parts[1] = strconv.Itoa(sniPort)
	newBody := strings.Join(parts, ":")
	newFull := newBody + params
	return "ssr://" + base64.RawURLEncoding.EncodeToString([]byte(newFull))
}

// ────────────────────────────────────────────────────────────────────────────

func writeOutputFiles(results []configResult) {
	byProto := make(map[string][]string)
	byProtoClash := make(map[string][]string)
	byProtoClashNames := make(map[string][]string)
	var all []string
	var allClash []string
	var allClashNames []string

	// SNI variants
	bySNIProto := make(map[string][]string)
	bySNIProtoClash := make(map[string][]string)
	bySNIProtoClashNames := make(map[string][]string)
	var allSNI []string
	var allSNIClash []string
	var allSNIClashNames []string

	const ownerName = "@DeltaKroneckerGithub"

	for _, r := range results {
		named := renameTo(r.line, r.proto, ownerName)
		all = append(all, named)
		byProto[r.proto] = append(byProto[r.proto], named)

		cname := ownerName
		if entry, ok := configToClashYAML(r.line, r.proto, cname); ok {
			allClash = append(allClash, entry)
			allClashNames = append(allClashNames, cname)
			byProtoClash[r.proto] = append(byProtoClash[r.proto], entry)
			byProtoClashNames[r.proto] = append(byProtoClashNames[r.proto], cname)
		}

		// Build SNI variant
		sniLine := toSNIConfig(r.line, r.proto)
		if sniLine != "" {
			sniNamed := renameTo(sniLine, r.proto, ownerName)
			allSNI = append(allSNI, sniNamed)
			bySNIProto[r.proto] = append(bySNIProto[r.proto], sniNamed)

			if sniEntry, ok := configToClashYAML(sniLine, r.proto, cname); ok {
				allSNIClash = append(allSNIClash, sniEntry)
				allSNIClashNames = append(allSNIClashNames, cname)
				bySNIProtoClash[r.proto] = append(bySNIProtoClash[r.proto], sniEntry)
				bySNIProtoClashNames[r.proto] = append(bySNIProtoClashNames[r.proto], cname)
			}
		}
	}

	// ── Write original output files ──────────────────────────────────────────
	writeFile(cfg.Output.MainFile, all)
	for proto, lines := range byProto {
		writeFile(filepath.Join(cfg.Output.ProtocolsDir, proto+".txt"), lines)
	}

	if gClash.simple != "" {
		writeClashConfigSimple(filepath.Join(filepath.Dir(cfg.Output.MainFile), "clash.yaml"), allClash, allClashNames)
		for proto, entries := range byProtoClash {
			writeClashConfigSimple(filepath.Join(cfg.Output.ProtocolsDir, proto+"_clash.yaml"), entries, byProtoClashNames[proto])
		}
	}
	if gClash.advanced != "" {
		writeClashConfigAdvanced(filepath.Join(filepath.Dir(cfg.Output.MainFile), "clash_advanced.yaml"), allClash, allClashNames)
		for proto, entries := range byProtoClash {
			writeClashConfigAdvanced(filepath.Join(cfg.Output.ProtocolsDir, proto+"_clash_advanced.yaml"), entries, byProtoClashNames[proto])
		}
	}

	// ── Write SNI output files ───────────────────────────────────────────────
	sniDir := "config/sni"
	sniProtosDir := "config/sni/protocols"

	writeFile(filepath.Join(sniDir, "all_configs_sni.txt"), allSNI)
	for proto, lines := range bySNIProto {
		writeFile(filepath.Join(sniProtosDir, proto+"_sni.txt"), lines)
	}

	if gClash.simple != "" {
		writeClashConfigSimple(filepath.Join(sniDir, "clash_sni.yaml"), allSNIClash, allSNIClashNames)
		for proto, entries := range bySNIProtoClash {
			writeClashConfigSimple(filepath.Join(sniProtosDir, proto+"_clash_sni.yaml"), entries, bySNIProtoClashNames[proto])
		}
	}
	if gClash.advanced != "" {
		writeClashConfigAdvanced(filepath.Join(sniDir, "clash_advanced_sni.yaml"), allSNIClash, allSNIClashNames)
		for proto, entries := range bySNIProtoClash {
			writeClashConfigAdvanced(filepath.Join(sniProtosDir, proto+"_clash_advanced_sni.yaml"), entries, bySNIProtoClashNames[proto])
		}
	}

	writeBatchFiles(all, allClash, allClashNames, allSNI, allSNIClash, allSNIClashNames)
}

func writeBatchFiles(
	allV2ray []string, allClash []string, allClashNames []string,
	allSNIV2ray []string, allSNIClash []string, allSNIClashNames []string,
) {
	const batchSize = 500

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))

	// ── Original V2ray batches ────────────────────────────────────────────────
	shuffledV2ray := make([]string, len(allV2ray))
	copy(shuffledV2ray, allV2ray)
	rng.Shuffle(len(shuffledV2ray), func(i, j int) { shuffledV2ray[i], shuffledV2ray[j] = shuffledV2ray[j], shuffledV2ray[i] })

	type clashPair struct {
		entry string
		name  string
	}
	shuffledClash := make([]clashPair, len(allClash))
	for i := range allClash {
		shuffledClash[i] = clashPair{entry: allClash[i], name: allClashNames[i]}
	}
	rng.Shuffle(len(shuffledClash), func(i, j int) { shuffledClash[i], shuffledClash[j] = shuffledClash[j], shuffledClash[i] })

	for batchIdx := 0; batchIdx*batchSize < len(shuffledV2ray); batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > len(shuffledV2ray) {
			end = len(shuffledV2ray)
		}
		writeFile(fmt.Sprintf("config/batches/v2ray/batch_%03d.txt", batchIdx+1), shuffledV2ray[start:end])
	}

	if len(shuffledClash) > 0 {
		for batchIdx := 0; batchIdx*batchSize < len(shuffledClash); batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(shuffledClash) {
				end = len(shuffledClash)
			}
			batch := shuffledClash[start:end]
			entries := make([]string, len(batch))
			names := make([]string, len(batch))
			for i, p := range batch {
				entries[i] = p.entry
				names[i] = p.name
			}
			if gClash.simple != "" {
				writeClashConfigSimple(fmt.Sprintf("config/batches/clash/batch_%03d.yaml", batchIdx+1), entries, names)
			}
			if gClash.advanced != "" {
				writeClashConfigAdvanced(fmt.Sprintf("config/batches/clash_advanced/batch_%03d.yaml", batchIdx+1), entries, names)
			}
		}
	}

	// ── SNI V2ray batches ─────────────────────────────────────────────────────
	shuffledSNIV2ray := make([]string, len(allSNIV2ray))
	copy(shuffledSNIV2ray, allSNIV2ray)
	rng.Shuffle(len(shuffledSNIV2ray), func(i, j int) { shuffledSNIV2ray[i], shuffledSNIV2ray[j] = shuffledSNIV2ray[j], shuffledSNIV2ray[i] })

	shuffledSNIClash := make([]clashPair, len(allSNIClash))
	for i := range allSNIClash {
		shuffledSNIClash[i] = clashPair{entry: allSNIClash[i], name: allSNIClashNames[i]}
	}
	rng.Shuffle(len(shuffledSNIClash), func(i, j int) { shuffledSNIClash[i], shuffledSNIClash[j] = shuffledSNIClash[j], shuffledSNIClash[i] })

	for batchIdx := 0; batchIdx*batchSize < len(shuffledSNIV2ray); batchIdx++ {
		start := batchIdx * batchSize
		end := start + batchSize
		if end > len(shuffledSNIV2ray) {
			end = len(shuffledSNIV2ray)
		}
		writeFile(fmt.Sprintf("config/batches/sni_v2ray/batch_%03d.txt", batchIdx+1), shuffledSNIV2ray[start:end])
	}

	if len(shuffledSNIClash) > 0 {
		for batchIdx := 0; batchIdx*batchSize < len(shuffledSNIClash); batchIdx++ {
			start := batchIdx * batchSize
			end := start + batchSize
			if end > len(shuffledSNIClash) {
				end = len(shuffledSNIClash)
			}
			batch := shuffledSNIClash[start:end]
			entries := make([]string, len(batch))
			names := make([]string, len(batch))
			for i, p := range batch {
				entries[i] = p.entry
				names[i] = p.name
			}
			if gClash.simple != "" {
				writeClashConfigSimple(fmt.Sprintf("config/batches/sni_clash/batch_%03d.yaml", batchIdx+1), entries, names)
			}
			if gClash.advanced != "" {
				writeClashConfigAdvanced(fmt.Sprintf("config/batches/sni_clash_advanced/batch_%03d.yaml", batchIdx+1), entries, names)
			}
		}
	}
}

func writeFile(path string, lines []string) {
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("❌ Cannot write %s: %v\n", path, err)
		return
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 256*1024)
	for _, line := range lines {
		w.WriteString(line + "\n")
	}
	w.Flush()
}

func writeClashConfigSimple(path string, proxyEntries, proxyNames []string) {
	if len(proxyEntries) == 0 || gClash.simple == "" {
		return
	}
	content := injectClashProxies(gClash.simple, proxyEntries, proxyNames)
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("❌ Cannot write %s: %v\n", path, err)
		return
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 512*1024)
	defer w.Flush()
	w.WriteString(content)
}

func writeClashConfigAdvanced(path string, proxyEntries, proxyNames []string) {
	if len(proxyEntries) == 0 || gClash.advanced == "" {
		return
	}
	content := injectClashProxies(gClash.advanced, proxyEntries, proxyNames)
	f, err := os.Create(path)
	if err != nil {
		fmt.Printf("❌ Cannot write %s: %v\n", path, err)
		return
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 512*1024)
	defer w.Flush()
	w.WriteString(content)
}

func configToClashYAML(line, proto, name string) (string, bool) {
	switch proto {
	case "vmess":
		return vmessClashYAML(line, name)
	case "vless":
		return vlessClashYAML(line, name)
	case "trojan":
		return trojanClashYAML(line, name)
	case "ss":
		return ssClashYAML(line, name)
	case "hy2":
		return hy2ClashYAML(line, name)
	case "hy":
		return hyClashYAML(line, name)
	case "tuic":
		return tuicClashYAML(line, name)
	case "ssr":
		return "", false
	}
	return "", false
}

func vmessClashYAML(raw, name string) (string, bool) {
	data := strings.TrimPrefix(raw, "vmess://")
	if idx := strings.LastIndex(data, "#"); idx != -1 {
		data = data[:idx]
	}
	data = strings.TrimSpace(data)

	var d map[string]interface{}

	if strings.HasPrefix(data, "{") {
		if err := json.Unmarshal([]byte(data), &d); err != nil {
			return "", false
		}
	} else {
		decoded, err := decodeBase64([]byte(data))
		if err == nil {
			if json.Unmarshal([]byte(decoded), &d) != nil {
				d = nil
			}
		}
		if d == nil {
			pd, parseErr := parseVMessURItoD(data)
			if parseErr != "" {
				return "", false
			}
			d = pd
		}
	}

	if d == nil {
		return "", false
	}
	server := strings.TrimSpace(fmt.Sprintf("%v", d["add"]))
	if server == "" {
		return "", false
	}
	port, err := toPort(fmt.Sprintf("%v", d["port"]))
	if err != nil {
		return "", false
	}
	uuid := strings.TrimSpace(fmt.Sprintf("%v", d["id"]))
	if uuid == "" {
		return "", false
	}
	alterId := 0
	if v, ok := d["aid"]; ok {
		switch x := v.(type) {
		case float64:
			alterId = int(x)
		case string:
			alterId, _ = strconv.Atoi(x)
		}
	}
	cipher := "auto"
	if s, _ := d["scy"].(string); s != "" {
		cipher = s
	}
	network := "tcp"
	if n, _ := d["net"].(string); n != "" {
		network = n
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: vmess\n    server: %s\n    port: %d\n    uuid: %s\n    alterId: %d\n    cipher: %s\n    udp: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(uuid), alterId, yamlQuote(cipher))
	if tlsVal, _ := d["tls"].(string); tlsVal == "tls" {
		sni := server
		if s, _ := d["sni"].(string); s != "" {
			sni = s
		} else if h, _ := d["host"].(string); h != "" {
			sni = h
		}
		fmt.Fprintf(&sb, "    tls: true\n    skip-cert-verify: true\n    servername: %s\n", yamlQuote(sni))
		if fp, _ := d["fp"].(string); fp != "" {
			fmt.Fprintf(&sb, "    client-fingerprint: %s\n", yamlQuote(fp))
		}
	}
	appendNetworkClash(&sb, network, strDefault(d["path"], "/"), strDefault(d["host"], ""),
		strDefault(d["serviceName"], strDefault(d["path"], "")))
	return sb.String(), true
}

func vlessClashYAML(raw, name string) (string, bool) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", false
	}
	uuid := u.User.Username()
	server := u.Hostname()
	port, err := toPort(u.Port())
	if err != nil || uuid == "" || server == "" {
		return "", false
	}
	q := u.Query()
	security := strings.ToLower(q.Get("security"))
	network := strings.ToLower(q.Get("type"))
	if network == "" {
		network = "tcp"
	}
	sni := first(q.Get("sni"), q.Get("peer"), server)
	fp := q.Get("fp")
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: vless\n    server: %s\n    port: %d\n    uuid: %s\n    udp: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(uuid))
	if flow := q.Get("flow"); flow != "" {
		fmt.Fprintf(&sb, "    flow: %s\n", yamlQuote(flow))
	}
	switch security {
	case "tls", "xtls":
		fmt.Fprintf(&sb, "    tls: true\n    skip-cert-verify: true\n    servername: %s\n", yamlQuote(sni))
		if fp != "" {
			fmt.Fprintf(&sb, "    client-fingerprint: %s\n", yamlQuote(fp))
		}
		if alpn := q.Get("alpn"); alpn != "" {
			parts := strings.Split(alpn, ",")
			quoted := make([]string, len(parts))
			for i, a := range parts {
				quoted[i] = yamlQuote(strings.TrimSpace(a))
			}
			fmt.Fprintf(&sb, "    alpn: [%s]\n", strings.Join(quoted, ", "))
		}
	case "reality":
		pbk := q.Get("pbk")
		if pbk == "" {
			return "", false
		}
		fmt.Fprintf(&sb, "    tls: true\n    skip-cert-verify: false\n    servername: %s\n    client-fingerprint: %s\n    reality-opts:\n      public-key: %s\n",
			yamlQuote(sni), yamlQuote(first(fp, "chrome")), yamlQuote(pbk))
		if sid := q.Get("sid"); sid != "" {
			fmt.Fprintf(&sb, "      short-id: %s\n", yamlQuote(sid))
		}
	}
	appendNetworkClash(&sb, network, first(q.Get("path"), "/"), q.Get("host"),
		first(q.Get("serviceName"), q.Get("path")))
	return sb.String(), true
}

func trojanClashYAML(raw, name string) (string, bool) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", false
	}
	password := u.User.Username()
	server := u.Hostname()
	port, err := toPort(u.Port())
	if err != nil || password == "" || server == "" {
		return "", false
	}
	q := u.Query()
	sni := first(q.Get("sni"), q.Get("peer"), server)
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: trojan\n    server: %s\n    port: %d\n    password: %s\n    sni: %s\n    skip-cert-verify: true\n    udp: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(password), yamlQuote(sni))
	if fp := q.Get("fp"); fp != "" {
		fmt.Fprintf(&sb, "    client-fingerprint: %s\n", yamlQuote(fp))
	}
	appendNetworkClash(&sb, strings.ToLower(q.Get("type")), first(q.Get("path"), "/"), q.Get("host"),
		first(q.Get("serviceName"), q.Get("path")))
	return sb.String(), true
}

func ssClashYAML(raw, name string) (string, bool) {
	trimmed := strings.TrimPrefix(raw, "ss://")
	queryStr := ""
	if idx := strings.Index(trimmed, "?"); idx != -1 {
		qEnd := len(trimmed)
		if fragIdx := strings.Index(trimmed[idx:], "#"); fragIdx != -1 {
			qEnd = idx + fragIdx
		}
		queryStr = trimmed[idx+1 : qEnd]
		trimmed = trimmed[:idx] + trimmed[qEnd:]
	}
	if idx := strings.Index(trimmed, "#"); idx != -1 {
		trimmed = trimmed[:idx]
	}
	atIdx := strings.LastIndex(trimmed, "@")
	var userInfo, hostInfo string
	if atIdx == -1 {
		decoded, err := decodeBase64([]byte(trimmed))
		if err != nil {
			return "", false
		}
		atIdx = strings.LastIndex(decoded, "@")
		if atIdx == -1 {
			return "", false
		}
		userInfo = decoded[:atIdx]
		hostInfo = decoded[atIdx+1:]
	} else {
		userInfo = trimmed[:atIdx]
		hostInfo = trimmed[atIdx+1:]
	}
	if idx := strings.Index(hostInfo, "?"); idx != -1 {
		hostInfo = hostInfo[:idx]
	}
	decoded, err := decodeBase64([]byte(userInfo))
	if err != nil {
		decoded = userInfo
	}
	parts := strings.SplitN(decoded, ":", 2)
	if len(parts) != 2 {
		return "", false
	}
	lastColon := strings.LastIndex(hostInfo, ":")
	if lastColon == -1 {
		return "", false
	}
	portStr := hostInfo[lastColon+1:]
	if s := strings.Index(portStr, "/"); s != -1 {
		portStr = portStr[:s]
	}
	server := hostInfo[:lastColon]
	port, err := toPort(portStr)
	if err != nil || server == "" {
		return "", false
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: ss\n    server: %s\n    port: %d\n    cipher: %s\n    password: %s\n    udp: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(parts[0]), yamlQuote(parts[1]))

	if queryStr != "" {
		q, _ := url.ParseQuery(queryStr)
		if pluginParam := q.Get("plugin"); pluginParam != "" {
			pluginParts := strings.SplitN(pluginParam, ";", 2)
			pluginName := pluginParts[0]
			switch {
			case pluginName == "obfs-local" || pluginName == "obfs":
				fmt.Fprintf(&sb, "    plugin: obfs\n    plugin-opts:\n")
				if len(pluginParts) > 1 {
					opts := parsePluginOpts(pluginParts[1])
					if mode, ok := opts["obfs"]; ok {
						fmt.Fprintf(&sb, "      mode: %s\n", yamlQuote(mode))
					}
					if host, ok := opts["obfs-host"]; ok {
						fmt.Fprintf(&sb, "      host: %s\n", yamlQuote(host))
					}
				}
			case pluginName == "v2ray-plugin":
				fmt.Fprintf(&sb, "    plugin: v2ray-plugin\n    plugin-opts:\n")
				if len(pluginParts) > 1 {
					opts := parsePluginOpts(pluginParts[1])
					mode := first(opts["mode"], "websocket")
					fmt.Fprintf(&sb, "      mode: %s\n", yamlQuote(mode))
					if path, ok := opts["path"]; ok {
						fmt.Fprintf(&sb, "      path: %s\n", yamlQuote(path))
					}
					if host, ok := opts["host"]; ok {
						fmt.Fprintf(&sb, "      host: %s\n", yamlQuote(host))
					}
					if _, hasTLS := opts["tls"]; hasTLS {
						fmt.Fprintf(&sb, "      tls: true\n")
					}
				}
			}
		}
	}
	return sb.String(), true
}

func parsePluginOpts(s string) map[string]string {
	opts := make(map[string]string)
	for _, part := range strings.Split(s, ";") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if idx := strings.Index(part, "="); idx != -1 {
			opts[part[:idx]] = part[idx+1:]
		} else {
			opts[part] = ""
		}
	}
	return opts
}

func hy2ClashYAML(raw, name string) (string, bool) {
	trimmed := strings.TrimPrefix(raw, "hy2://")
	if i := strings.LastIndex(trimmed, "#"); i != -1 {
		trimmed = trimmed[:i]
	}
	queryStr := ""
	if i := strings.Index(trimmed, "?"); i != -1 {
		queryStr = trimmed[i+1:]
		trimmed = trimmed[:i]
	}
	lastAt := strings.LastIndex(trimmed, "@")
	if lastAt == -1 {
		return "", false
	}
	password := trimmed[:lastAt]
	hostPort := trimmed[lastAt+1:]
	if password == "" {
		return "", false
	}
	if i := strings.Index(hostPort, "/"); i != -1 {
		hostPort = hostPort[:i]
	}
	lastColon := strings.LastIndex(hostPort, ":")
	if lastColon == -1 {
		return "", false
	}
	server := hostPort[:lastColon]
	port, err := toPort(hostPort[lastColon+1:])
	if err != nil || server == "" {
		return "", false
	}
	q, _ := url.ParseQuery(queryStr)
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: hysteria2\n    server: %s\n    port: %d\n    password: %s\n    sni: %s\n    skip-cert-verify: true\n    udp: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(password), yamlQuote(first(q.Get("sni"), server)))
	if obfs := q.Get("obfs"); obfs != "" {
		fmt.Fprintf(&sb, "    obfs: %s\n", yamlQuote(obfs))
		if obfsPwd := q.Get("obfs-password"); obfsPwd != "" {
			fmt.Fprintf(&sb, "    obfs-password: %s\n", yamlQuote(obfsPwd))
		}
	}
	return sb.String(), true
}

func hyClashYAML(raw, name string) (string, bool) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", false
	}
	server := u.Hostname()
	if server == "" {
		return "", false
	}
	port, err := toPort(u.Port())
	if err != nil {
		return "", false
	}
	q := u.Query()
	auth := first(q.Get("auth"), u.User.Username())
	if auth == "" {
		return "", false
	}
	up, _ := strconv.Atoi(first(q.Get("upmbps"), "10"))
	down, _ := strconv.Atoi(first(q.Get("downmbps"), "50"))
	if up <= 0 {
		up = 10
	}
	if down <= 0 {
		down = 50
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: hysteria\n    server: %s\n    port: %d\n    auth-str: %s\n    up: %d\n    down: %d\n    sni: %s\n    skip-cert-verify: true\n    udp: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(auth), up, down,
		yamlQuote(first(q.Get("peer"), q.Get("sni"), server)))
	if obfs := q.Get("obfs"); obfs != "" {
		fmt.Fprintf(&sb, "    obfs: %s\n", yamlQuote(obfs))
	}
	if proto := q.Get("protocol"); proto != "" {
		fmt.Fprintf(&sb, "    protocol: %s\n", yamlQuote(proto))
	}
	if alpnStr := q.Get("alpn"); alpnStr != "" {
		parts := strings.Split(alpnStr, ",")
		quoted := make([]string, len(parts))
		for i, a := range parts {
			quoted[i] = yamlQuote(strings.TrimSpace(a))
		}
		fmt.Fprintf(&sb, "    alpn: [%s]\n", strings.Join(quoted, ", "))
	}
	return sb.String(), true
}

func tuicClashYAML(raw, name string) (string, bool) {
	u, err := url.Parse(sanitizeProxyURL(raw))
	if err != nil {
		return "", false
	}
	uuid := u.User.Username()
	password, _ := u.User.Password()
	server := u.Hostname()
	port, err := toPort(u.Port())
	if err != nil || uuid == "" || server == "" {
		return "", false
	}
	q := u.Query()
	congestion := first(q.Get("congestion_control"), q.Get("congestion-controller"), "bbr")
	var sb strings.Builder
	fmt.Fprintf(&sb, "  - name: %s\n    type: tuic\n    server: %s\n    port: %d\n    uuid: %s\n    password: %s\n    sni: %s\n    skip-cert-verify: true\n    udp: true\n    congestion-controller: %s\n    reduce-rtt: true\n",
		yamlQuote(name), yamlQuote(server), port, yamlQuote(uuid), yamlQuote(password),
		yamlQuote(first(q.Get("sni"), server)), congestion)
	if udpRelay := first(q.Get("udp_relay_mode"), q.Get("udp-relay-mode")); udpRelay != "" {
		fmt.Fprintf(&sb, "    udp-relay-mode: %s\n", yamlQuote(udpRelay))
	}
	return sb.String(), true
}

func appendNetworkClash(sb *strings.Builder, network, path, host, grpcService string) {
	if path == "" {
		path = "/"
	}
	switch network {
	case "ws":
		fmt.Fprintf(sb, "    network: ws\n    ws-opts:\n      path: %s\n", yamlQuote(path))
		if host != "" {
			fmt.Fprintf(sb, "      headers:\n        Host: %s\n", yamlQuote(host))
		}
	case "grpc":
		fmt.Fprintf(sb, "    network: grpc\n    grpc-opts:\n      grpc-service-name: %s\n", yamlQuote(grpcService))
	case "h2", "http":
		fmt.Fprintf(sb, "    network: h2\n    h2-opts:\n      path: %s\n", yamlQuote(path))
		if host != "" {
			fmt.Fprintf(sb, "      host: [%s]\n", yamlQuote(host))
		}
	case "httpupgrade":
		fmt.Fprintf(sb, "    network: httpupgrade\n    httpupgrade-opts:\n      path: %s\n", yamlQuote(path))
		if host != "" {
			fmt.Fprintf(sb, "      host: %s\n", yamlQuote(host))
		}
	case "splithttp", "xhttp":
		fmt.Fprintf(sb, "    network: splithttp\n    splithttp-opts:\n      path: %s\n", yamlQuote(path))
		if host != "" {
			fmt.Fprintf(sb, "      host: %s\n", yamlQuote(host))
		}
	}
}

func strDefault(v interface{}, def string) string {
	if v == nil {
		return def
	}
	s, ok := v.(string)
	if !ok || s == "" {
		return def
	}
	return s
}

func renameTo(config, protocol, newName string) string {
	switch protocol {
	case "vmess":
		data := strings.TrimPrefix(config, "vmess://")
		fragIdx := strings.LastIndex(data, "#")
		if fragIdx != -1 {
			data = data[:fragIdx]
		}
		data = strings.TrimSpace(data)

		isURI := false
		if strings.HasPrefix(data, "{") {
			isURI = false
		} else {
			decoded, err := decodeBase64([]byte(data))
			if err == nil {
				var tmp map[string]interface{}
				if json.Unmarshal([]byte(decoded), &tmp) == nil {
					tmp["ps"] = newName
					keys := make([]string, 0, len(tmp))
					for k := range tmp {
						keys = append(keys, k)
					}
					sort.Strings(keys)
					var buf bytes.Buffer
					buf.WriteByte('{')
					for i, k := range keys {
						if i > 0 {
							buf.WriteByte(',')
						}
						kj, _ := json.Marshal(k)
						vj, _ := json.Marshal(tmp[k])
						buf.Write(kj)
						buf.WriteByte(':')
						buf.Write(vj)
					}
					buf.WriteByte('}')
					return "vmess://" + base64.StdEncoding.EncodeToString(buf.Bytes())
				}
			}
			if atIdx := strings.Index(data, "@"); atIdx != -1 {
				isURI = true
			}
		}

		if !isURI {
			var d map[string]interface{}
			if err := json.Unmarshal([]byte(data), &d); err != nil {
				return config
			}
			d["ps"] = newName
			keys := make([]string, 0, len(d))
			for k := range d {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var buf bytes.Buffer
			buf.WriteByte('{')
			for i, k := range keys {
				if i > 0 {
					buf.WriteByte(',')
				}
				kj, _ := json.Marshal(k)
				vj, _ := json.Marshal(d[k])
				buf.Write(kj)
				buf.WriteByte(':')
				buf.Write(vj)
			}
			buf.WriteByte('}')
			return "vmess://" + base64.StdEncoding.EncodeToString(buf.Bytes())
		}

		return "vmess://" + data + "#" + url.PathEscape(newName)

	default:
		if idx := strings.Index(config, "#"); idx != -1 {
			return config[:idx] + "#" + url.PathEscape(newName)
		}
		return config + "#" + url.PathEscape(newName)
	}
}

func countBatchFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			count++
		}
	}
	return count
}

func min500(batchIdx, total int) int {
	start := (batchIdx - 1) * 500
	if start >= total {
		return 0
	}
	end := start + 500
	if end > total {
		return total - start
	}
	return end - start
}




func writeSummary(results []configResult, failedLinks []string, duration float64, originalTotal int) {
	byProtoOut := make(map[string]int)
	for _, r := range results {
		byProtoOut[r.proto]++
	}

	f, err := os.Create("README.md")
	if err != nil {
		return
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	defer w.Flush()

	repoBase := "https://github.com/Delta-Kronecker/V2ray-Config/raw/refs/heads/main"

	// ── SINGLE SNI TABLE (everything together) ────────────────────────────────────
	w.WriteString("## SNI-Spoofing Configs (server=127.0.0.1, port=40443)\n\n")
	w.WriteString("> https://t.me/DeltaSNI برای اموزش و گفت و گو درباره روش های مبتنی بر این روش در گروه تلگرامی ما عضو بشید \n")
	w.WriteString("> لطفا هر پروژه ای براتون مفید بود حتما استار بدید، با این کار انگیزه توسعه دهنده برای ادامه رو تامین می کنید🫀 \n")

	fmt.Fprintf(w, "| Type | Count | Link |\n|---|---|---|\n")
	
	// Main SNI file
	fmt.Fprintf(w, "| All SNI configs | — | [all_configs_sni.txt](%s/config/sni/all_configs_sni.txt) |\n", repoBase)
	
	// SNI by protocol
	for _, p := range cfg.ProtocolOrder {
		if n := byProtoOut[p]; n > 0 {
			fmt.Fprintf(w, "| Protocol: %s | %d | [%s_sni.txt](%s/config/sni/protocols/%s_sni.txt) |\n",
				strings.ToUpper(p), n, p, repoBase, p)
		}
	}
	
	// SNI V2ray Batches
	sniV2rayBatches := countBatchFiles("config/batches/sni_v2ray")
	for i := 1; i <= sniV2rayBatches; i++ {
		cnt := min500(i, len(results))
		fmt.Fprintf(w, "| Batch %03d | %d | [batch_%03d.txt](%s/config/batches/sni_v2ray/batch_%03d.txt) |\n",
			i, cnt, i, repoBase, i)
	}
	
	w.WriteString("\n---\n\n")
	
	// ── Main Files (V2ray + Clash, no SNI) ────────────────────────────────────────
	w.WriteString("## Main Files\n\n")

	w.WriteString("### V2ray — All Configs\n\n")
	fmt.Fprintf(w, "| File | Link |\n|---|---|\n")
	fmt.Fprintf(w, "| All configs (txt) | [all_configs.txt](%s/config/all_configs.txt) |\n\n", repoBase)

	w.WriteString("### V2ray — By Protocol\n\n")
	fmt.Fprintf(w, "| Protocol | Count | Link |\n|---|---|---|\n")
	for _, p := range cfg.ProtocolOrder {
		if n := byProtoOut[p]; n > 0 {
			fmt.Fprintf(w, "| %s | %d | [%s.txt](%s/config/protocols/%s.txt) |\n",
				strings.ToUpper(p), n, p, repoBase, p)
		}
	}
	w.WriteString("\n")

	w.WriteString("### Clash \n\n")
	fmt.Fprintf(w, "Groups: **PROXY** (selector) → **Load-Balance** · **Auto** · **Fallback**\n\n")
	fmt.Fprintf(w, "| File | Link |\n|---|---|\n")
	fmt.Fprintf(w, "| clash.yaml (all protocols) | [clash.yaml](%s/config/clash.yaml) |\n", repoBase)
	for _, p := range cfg.ProtocolOrder {
		if byProtoOut[p] > 0 {
			fmt.Fprintf(w, "| %s_clash.yaml | [%s_clash.yaml](%s/config/protocols/%s_clash.yaml) |\n",
				p, p, repoBase, p)
		}
	}
	w.WriteString("\n")

	w.WriteString("---\n\n")
	
	// ── Batch Files (only regular V2ray and Clash, NO SNI batches) ────────────────
	w.WriteString("## Batch Files — Random 500-Config Groups\n\n")
	w.WriteString("> Each file contains 500 randomly selected configs from all protocols.\n\n")

	v2rayBatches := countBatchFiles("config/batches/v2ray")
	clashBatches := countBatchFiles("config/batches/clash")

	w.WriteString("### V2ray Batches\n\n")
	fmt.Fprintf(w, "| Batch | Count | Link |\n|---|---|---|\n")
	for i := 1; i <= v2rayBatches; i++ {
		cnt := min500(i, len(results))
		fmt.Fprintf(w, "| Batch %03d | %d | [batch_%03d.txt](%s/config/batches/v2ray/batch_%03d.txt) |\n",
			i, cnt, i, repoBase, i)
	}
	w.WriteString("\n")

	w.WriteString("### Clash Batches\n\n")
	fmt.Fprintf(w, "| Batch | Link |\n|---|---|\n")
	for i := 1; i <= clashBatches; i++ {
		fmt.Fprintf(w, "| Batch %03d | [batch_%03d.yaml](%s/config/batches/clash/batch_%03d.yaml) |\n",
			i, i, repoBase, i)
	}
	w.WriteString("\n")

	w.WriteString("---\n\n")
	
	// ── Statistics ────────────────────────────────────────────────────────────────
	w.WriteString("## Statistics\n\n")

	w.WriteString("### Per-Protocol Input & Output\n\n")
	fmt.Fprintf(w, "| Protocol | Tested (unique) | valid | Pass Rate |\n|---|---|---|---|\n")
	totalIn := 0
	totalOut := 0
	for _, p := range cfg.ProtocolOrder {
		in := gInputByProto[p]
		out := byProtoOut[p]
		totalIn += in
		totalOut += out
		rate := 0.0
		if in > 0 {
			rate = float64(out) / float64(in) * 100
		}
		fmt.Fprintf(w, "| %s | %d | %d | %.1f%% |\n", strings.ToUpper(p), in, out, rate)
	}
	overallRate := 0.0
	if totalIn > 0 {
		overallRate = float64(totalOut) / float64(totalIn) * 100
	}
	fmt.Fprintf(w, "| **Total** | **%d** | **%d** | **%.1f%%** |\n\n", totalIn, totalOut, overallRate)

	fmt.Fprintf(w, "| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(w, "| Raw fetched lines | %d |\n", originalTotal)
	fmt.Fprintf(w, "| Unique after dedup | %d |\n", totalIn)
	fmt.Fprintf(w, "| Valid configs | %d |\n", len(results))
	fmt.Fprintf(w, "| Processing time | %.2fs |\n\n", duration)

	w.WriteString("---\n\n")
	w.WriteString("## 🔥 Keep This Project Going!\n\n")
	w.WriteString("If you're finding this useful, please show your support with:\n\n")
	w.WriteString("⭐ **Star the repository **\n\n")
	w.WriteString("Your stars fuel our motivation to keep improving!\n")
}







func decodeBase64(encoded []byte) (string, error) {
	s := strings.Map(func(r rune) rune {
		if r == ' ' || r == '\t' || r == '\r' || r == '\n' {
			return -1
		}
		return r
	}, string(encoded))

	stripped := strings.TrimRight(s, "=")

	padded := stripped
	if r := len(padded) % 4; r != 0 {
		padded += strings.Repeat("=", 4-r)
	}

	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.URLEncoding} {
		if b, err := enc.DecodeString(padded); err == nil {
			return string(b), nil
		}
	}
	for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
		if b, err := enc.DecodeString(stripped); err == nil {
			return string(b), nil
		}
	}
	_, err := base64.RawURLEncoding.DecodeString(stripped)
	return "", err
}

func toPort(s string) (int, error) {
	s = strings.TrimSpace(s)
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 || n > 65535 {
		return 0, fmt.Errorf("invalid port %q", s)
	}
	return n, nil
}

func first(values ...string) string {
	for _, v := range values {
		if v != "" {
			return v
		}
	}
	return ""
}

func singBoxPath() string {
	for _, p := range []string{"./sing-box", "/usr/local/bin/sing-box"} {
		if _, err := os.Stat(p); err == nil {
			return p
		}
	}
	return "sing-box"
}

func killGroup(cmd *exec.Cmd) {
	if cmd.Process == nil {
		return
	}
	pid := cmd.Process.Pid
	if pgid, err := syscall.Getpgid(pid); err == nil {
		syscall.Kill(-pgid, syscall.SIGKILL)
	}
	cmd.Process.Kill()
	done := make(chan struct{})
	go func() {
		cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		if pgid, err := syscall.Getpgid(pid); err == nil {
			syscall.Kill(-pgid, syscall.SIGKILL)
		}
		syscall.Kill(pid, syscall.SIGKILL)
	}
}

func countSingboxProcs() int {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		out, err2 := exec.Command("pgrep", "-c", "sing-box").Output()
		if err2 != nil {
			return -1
		}
		n, _ := strconv.Atoi(strings.TrimSpace(string(out)))
		return n
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		name := e.Name()
		allDigit := true
		for _, c := range name {
			if c < '0' || c > '9' {
				allDigit = false
				break
			}
		}
		if !allDigit {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + name + "/cmdline")
		if err != nil {
			continue
		}
		comm := strings.ReplaceAll(string(cmdline), "\x00", " ")
		if strings.Contains(comm, "sing-box") {
			count++
		}
	}
	return count
}

func readProcNetTCPPorts() map[int]bool {
	ports := make(map[int]bool)
	for _, f := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		for i, line := range strings.Split(string(data), "\n") {
			if i == 0 || strings.TrimSpace(line) == "" {
				continue
			}
			fields := strings.Fields(line)
			if len(fields) < 4 {
				continue
			}
			stateHex := fields[3]
			if stateHex != "0A" {
				continue
			}
			localAddr := fields[1]
			colonIdx := strings.LastIndex(localAddr, ":")
			if colonIdx == -1 {
				continue
			}
			portHex := localAddr[colonIdx+1:]
			portVal, err := strconv.ParseInt(portHex, 16, 32)
			if err != nil {
				continue
			}
			ports[int(portVal)] = true
		}
	}
	return ports
}

func checkOccupiedPorts(basePort, count int) []int {
	listeningPorts := readProcNetTCPPorts()
	var occupied []int
	for i := 0; i < count; i++ {
		p := basePort + i
		if listeningPorts[p] {
			occupied = append(occupied, p)
		}
	}
	return occupied
}

func extractErr(stderr string) string {
	var errs []string
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if strings.Contains(lower, "warn") || strings.Contains(lower, "deprecated") {
			continue
		}
		if strings.Contains(lower, `"level":"info"`) || strings.Contains(lower, `"level":"debug"`) ||
			strings.Contains(lower, "level=info") || strings.Contains(lower, "level=debug") {
			continue
		}
		if len(line) > 120 {
			line = line[:120] + "..."
		}
		errs = append(errs, line)
		if len(errs) >= 3 {
			break
		}
	}
	return strings.Join(errs, " | ")
}

func extractErrVerbose(stderr string) string {
	var first, best string
	priority := []string{"invalid", "failed", "decode", "unsupported", "error"}
	for _, line := range strings.Split(stderr, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		lower := strings.ToLower(line)
		if strings.Contains(lower, "warn") || strings.Contains(lower, "deprecated") {
			continue
		}
		if strings.Contains(lower, `"level":"info"`) || strings.Contains(lower, `"level":"debug"`) ||
			strings.Contains(lower, "level=info") || strings.Contains(lower, "level=debug") {
			continue
		}
		if idx := strings.Index(line, `"msg":"`); idx != -1 {
			end := strings.Index(line[idx+7:], `"`)
			if end != -1 {
				line = line[idx+7 : idx+7+end]
				lower = strings.ToLower(line)
			}
		}
		if first == "" {
			first = line
		}
		if best == "" {
			for _, kw := range priority {
				if strings.Contains(lower, kw) {
					best = line
					break
				}
			}
		}
	}
	r := best
	if r == "" {
		r = first
	}
	if len(r) > 180 {
		r = r[:180] + "..."
	}
	return r
}

func shortenErr(s string) string {
	s = strings.ReplaceAll(s, `"`, "")
	if strings.HasPrefix(s, "Get ") {
		if i := strings.Index(s, ": "); i != -1 && i > 10 {
			real := s[i+2:]
			s = real
		}
	}
	if len(s) > 100 {
		return s[:100] + "..."
	}
	return s
}

func yamlQuote(s string) string {
	s = strings.ReplaceAll(s, `\`, `\\`)
	s = strings.ReplaceAll(s, `"`, `\"`)
	return `"` + s + `"`
}
