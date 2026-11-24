package cfprobe

// X-UI 面板日报采集（探针接手面板 @daily 日报的 agent 侧）。
// 全部只读；任何一项取不到就留空/0，不阻塞原有上报。
// 口径以 MHSanaei/3x-ui main 分支为准（见实施规格 agent-fork-implementation-spec.md）。

import (
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/xtls/xray-core/app/stats/command"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	_ "modernc.org/sqlite"
)

const (
	xuiPanelBinary    = "/usr/local/x-ui/x-ui"
	xuiXrayBinary     = "/usr/local/x-ui/bin/xray-linux-amd64"
	xuiDBPath         = "/etc/x-ui/x-ui.db"
	xuiAPIConfigPath  = "/usr/local/x-ui/bin/config.json"
	xuiAPIFallback    = "127.0.0.1:62789"
	xuiOnlineGrace    = 20 * time.Second // 与面板 onlineGracePeriodMs 一致
	xuiPollInterval   = 5 * time.Second  // 与面板 XrayTrafficJob 的 poll 间隔一致
	xuiGRPCTimeout    = 3 * time.Second
	xuiCommandTimeout = 5 * time.Second
	xuiVersionCache   = time.Hour // 面板/xray 版本 1 小时缓存，避免每 tick 起两个进程
)

type xuiStats struct {
	Hostname           string
	PanelVersion       string
	XrayVersion        string
	XrayState          string // "Running" / "Stop"
	OnlineClients      int
	OnlineClientEmails string // 逗号分隔，已排序
	InboundsDisabled   int
	InboundsDepleted   int
	ClientsDisabled    int
	ClientsDepleted    int
}

// xuiOnlineTracker 每 5 秒 poll 一次 xray StatsService，记录每个 email 最近一次
// 有流量 delta 的时间；取数时保留 20 秒宽限内的集合。
type xuiOnlineTracker struct {
	mu           sync.Mutex
	lastSeen     map[string]time.Time
	lastVals     map[string]int64
	addr         string
	log          logger
	panelVersion string
	xrayVersion  string
	versionsAt   time.Time
}

func newXUIOnlineTracker(l logger) *xuiOnlineTracker {
	return &xuiOnlineTracker{
		lastSeen: make(map[string]time.Time),
		lastVals: make(map[string]int64),
		addr:     xuiAPIAddr(),
		log:      l,
	}
}

func (t *xuiOnlineTracker) run(ctx context.Context) {
	t.poll() // 立即 poll 一次，不等第一个 5 秒
	ticker := time.NewTicker(xuiPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.poll()
		}
	}
}

var xuiClientStatRE = regexp.MustCompile(`^user>>>([^>]+)>>>traffic>>>(downlink|uplink)$`)

func (t *xuiOnlineTracker) poll() {
	now := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), xuiGRPCTimeout)
	defer cancel()
	conn, err := grpc.NewClient(t.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.log.debugf("xui grpc dial failed addr=%s err=%v", t.addr, err)
		return
	}
	defer conn.Close()
	client := command.NewStatsServiceClient(conn)
	resp, err := client.QueryStats(ctx, &command.QueryStatsRequest{Pattern: "", Reset_: false})
	if err != nil {
		t.log.debugf("xui grpc QueryStats failed err=%v", err)
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	for _, st := range resp.GetStat() {
		m := xuiClientStatRE.FindStringSubmatch(st.GetName())
		if m == nil {
			continue
		}
		email := m[1]
		if st.GetValue() > t.lastVals[st.GetName()] {
			t.lastSeen[email] = now
		}
		t.lastVals[st.GetName()] = st.GetValue()
	}
	// 补充信号：连接态在线用户（能覆盖连着但 5 秒内无流量的客户端）。
	// 老 core 不支持该 RPC 时返回 Unimplemented，只用 delta 即可。
	if users, err := client.GetAllOnlineUsers(ctx, &command.GetAllOnlineUsersRequest{}); err == nil {
		for _, email := range users.GetUsers() {
			email = strings.TrimSpace(email)
			if email != "" {
				t.lastSeen[email] = now
			}
		}
	} else {
		t.log.debugf("xui grpc GetAllOnlineUsers failed (delta only) err=%v", err)
	}
}

func (t *xuiOnlineTracker) snapshot() []string {
	now := time.Now()
	t.mu.Lock()
	defer t.mu.Unlock()
	var online []string
	for email, ts := range t.lastSeen {
		if now.Sub(ts) < xuiOnlineGrace {
			online = append(online, email)
		}
	}
	sort.Strings(online)
	return online
}

// versions 返回面板/xray 版本（1 小时缓存）；tracker 为 nil 时直接现取。
func (t *xuiOnlineTracker) versions() (panel, xray string) {
	if t == nil {
		return xuiPanelVersion(), xuiXrayVersion()
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if !t.versionsAt.IsZero() && time.Since(t.versionsAt) < xuiVersionCache {
		return t.panelVersion, t.xrayVersion
	}
	t.panelVersion = xuiPanelVersion()
	t.xrayVersion = xuiXrayVersion()
	t.versionsAt = time.Now()
	return t.panelVersion, t.xrayVersion
}

// xuiAPIAddr 从 x-ui 的 config.json 读 tag=="api" 的 inbound 地址，读不到时 fallback。
func xuiAPIAddr() string {
	data, err := os.ReadFile(xuiAPIConfigPath)
	if err != nil {
		return xuiAPIFallback
	}
	var cfg struct {
		Inbounds []struct {
			Listen string `json:"listen"`
			Port   int    `json:"port"`
			Tag    string `json:"tag"`
		} `json:"inbounds"`
	}
	if err := json.Unmarshal(data, &cfg); err != nil {
		return xuiAPIFallback
	}
	for _, in := range cfg.Inbounds {
		if in.Tag == "api" && in.Port > 0 {
			host := in.Listen
			if host == "" {
				host = "127.0.0.1"
			}
			return host + ":" + strconv.Itoa(in.Port)
		}
	}
	return xuiAPIFallback
}

func xuiPanelVersion() string {
	out, err := commandOutputWithTimeout(xuiCommandTimeout, xuiPanelBinary, "-v")
	if err != nil {
		return ""
	}
	return strings.TrimSpace(out)
}

func xuiXrayVersion() string {
	out, err := commandOutputWithTimeout(xuiCommandTimeout, xuiXrayBinary, "-version")
	if err != nil {
		return ""
	}
	// 输出形如 "Xray 26.10.21 ... go1.27.3 linux/amd64"，取第 2 个 token（与面板 refreshVersion 同款解析）。
	fields := strings.Fields(out)
	if len(fields) >= 2 {
		return fields[1]
	}
	return ""
}

func xuiXrayRunning() bool {
	entries, err := os.ReadDir("/proc")
	if err != nil {
		return false
	}
	for _, e := range entries {
		if _, err := strconv.Atoi(e.Name()); err != nil {
			continue
		}
		cmdline, err := os.ReadFile("/proc/" + e.Name() + "/cmdline")
		if err != nil {
			continue
		}
		// cmdline 以 \0 分隔参数，第一个就是可执行文件路径。
		for _, p := range strings.Split(string(cmdline), "\x00") {
			if strings.HasSuffix(p, "xray-linux-amd64") {
				return true
			}
		}
	}
	return false
}

// xuiDepleteStats 只读 x-ui.db，按面板 depleteReport 的口径算耗尽数。
// 阈值 trafficDiff(GB)/expireDiff(天) 只有 >0 时才生效，否则为 0。
func xuiDepleteStats(l logger) (inDisabled, inDepleted, clDisabled, clDepleted int) {
	db, err := sql.Open("sqlite", "file:"+xuiDBPath+"?mode=ro&_busy_timeout=5000")
	if err != nil {
		l.debugf("xui db open failed err=%v", err)
		return 0, 0, 0, 0
	}
	defer db.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var trafficDiff, expireDiff float64
	rows, err := db.QueryContext(ctx, "SELECT key, value FROM settings WHERE key IN ('trafficDiff','expireDiff')")
	if err != nil {
		l.debugf("xui db settings query failed err=%v", err)
		return 0, 0, 0, 0
	}
	for rows.Next() {
		var key, val string
		if err := rows.Scan(&key, &val); err != nil {
			continue
		}
		n, _ := strconv.ParseFloat(strings.TrimSpace(val), 64)
		switch key {
		case "trafficDiff":
			trafficDiff = n
		case "expireDiff":
			expireDiff = n
		}
	}
	rows.Close()

	var trafficThreshold, expireThreshold int64
	if trafficDiff > 0 {
		trafficThreshold = int64(trafficDiff * 1073741824)
	}
	if expireDiff > 0 {
		expireThreshold = int64(expireDiff * 86400000)
	}
	nowMs := time.Now().UnixMilli()

	// 与面板 depleteReport 一致：!Enable → 禁用；否则过期或超额 → 即将耗尽。
	depleted := func(enable bool, expiryTime, total, up, down int64) (disabled, depleted bool) {
		if !enable {
			return true, false
		}
		if expiryTime > 0 && expiryTime-nowMs < expireThreshold {
			return false, true
		}
		if total > 0 && total-(up+down) < trafficThreshold {
			return false, true
		}
		return false, false
	}

	inRows, err := db.QueryContext(ctx, "SELECT enable, expiry_time, total, up, down FROM inbounds")
	if err != nil {
		l.debugf("xui db inbounds query failed err=%v", err)
		return 0, 0, 0, 0
	}
	for inRows.Next() {
		var enable int
		var expiryTime, total, up, down int64
		if err := inRows.Scan(&enable, &expiryTime, &total, &up, &down); err != nil {
			continue
		}
		d, p := depleted(enable != 0, expiryTime, total, up, down)
		if d {
			inDisabled++
		} else if p {
			inDepleted++
		}
	}
	inRows.Close()

	clRows, err := db.QueryContext(ctx, "SELECT email, enable, expiry_time, total, up, down FROM client_traffics")
	if err != nil {
		l.debugf("xui db client_traffics query failed err=%v", err)
		return inDisabled, inDepleted, 0, 0
	}
	seen := make(map[string]struct{})
	for clRows.Next() {
		var email string
		var enable int
		var expiryTime, total, up, down int64
		if err := clRows.Scan(&email, &enable, &expiryTime, &total, &up, &down); err != nil {
			continue
		}
		if _, dup := seen[email]; dup {
			continue
		}
		seen[email] = struct{}{}
		d, p := depleted(enable != 0, expiryTime, total, up, down)
		if d {
			clDisabled++
		} else if p {
			clDepleted++
		}
	}
	clRows.Close()
	return inDisabled, inDepleted, clDisabled, clDepleted
}

// collectXUIStats 采集全部 x-ui 字段；x-ui 不存在时全部留空/0，不报错。
func collectXUIStats(l logger, online *xuiOnlineTracker) xuiStats {
	var s xuiStats
	if hostname, err := os.Hostname(); err == nil {
		s.Hostname = hostname
	}
	s.PanelVersion, s.XrayVersion = online.versions()
	if xuiXrayRunning() {
		s.XrayState = "Running"
	} else {
		s.XrayState = "Stop"
	}
	if online != nil {
		emails := online.snapshot()
		s.OnlineClients = len(emails)
		s.OnlineClientEmails = strings.Join(emails, ",")
	}
	s.InboundsDisabled, s.InboundsDepleted, s.ClientsDisabled, s.ClientsDepleted = xuiDepleteStats(l)
	l.debugf("xui stats hostname=%s panel=%s xray=%s state=%s online=%d inbounds_disabled=%d inbounds_depleted=%d clients_disabled=%d clients_depleted=%d",
		s.Hostname, s.PanelVersion, s.XrayVersion, s.XrayState, s.OnlineClients,
		s.InboundsDisabled, s.InboundsDepleted, s.ClientsDisabled, s.ClientsDepleted)
	return s
}
