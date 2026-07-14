package main

import (
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const (
	apiBase  = "https://realspeed-v4.eu1.sk.thousandeyes.com"
	apiBase2 = "https://realspeed.eu1.sk.thousandeyes.com"
	wsHost   = "realspeed.eu1.sk.thousandeyes.com"
	wsPort   = 443
	targetsAPI = "https://speedtest-api.samknows.com/targets?targetSet=realspeed-1step"
	dlPort   = 6500
	ulPort   = 80
	ua       = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/150.0.0.0 Safari/537.36"
)

var debug bool

func dbg(f string, a ...any) {
	if debug {
		fmt.Fprintf(os.Stderr, "[dbg] "+f+"\n", a...)
	}
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

var client = &http.Client{
	Timeout: 15 * time.Second,
	Transport: &http.Transport{
		TLSClientConfig: &tls.Config{},
		DialContext: (&net.Dialer{
			Timeout:   5 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		MaxIdleConns:        10,
		MaxIdleConnsPerHost: 10,
		TLSHandshakeTimeout:   5 * time.Second,
		ResponseHeaderTimeout: 5 * time.Second,
	},
}

func httpGet(url string) (json.RawMessage, error) {
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", ua)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return json.RawMessage(b), nil
}

func httpPost(url string, body any) (json.RawMessage, error) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", ua)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return json.RawMessage(rb), nil
}

func httpPostAuth(url string, body any, token string) (json.RawMessage, error) {
	b, _ := json.Marshal(body)
	req, _ := http.NewRequest("POST", url, strings.NewReader(string(b)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", ua)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	return json.RawMessage(rb), nil
}

// ---------------------------------------------------------------------------
// Minimal WebSocket client (OpenSSL + raw frames)
// ---------------------------------------------------------------------------

func b64enc(d []byte) string {
	return base64.StdEncoding.EncodeToString(d)
}

type wsConn struct {
	conn net.Conn
}

func wsConnect(host string, port int, path string, token string) (*wsConn, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	tlsConn, err := tls.DialWithDialer(
		&net.Dialer{Timeout: 10 * time.Second},
		"tcp", addr,
		&tls.Config{ServerName: host},
	)
	if err != nil {
		return nil, fmt.Errorf("tls dial: %w", err)
	}

	nonce := make([]byte, 16)
	rand.Read(nonce)
	key := b64enc(nonce)

	wsPath := path + "?auth=" + token
	handshake := fmt.Sprintf("GET %s HTTP/1.1\r\n"+
		"Host: %s\r\n"+
		"Upgrade: websocket\r\n"+
		"Connection: Upgrade\r\n"+
		"Sec-WebSocket-Key: %s\r\n"+
		"Sec-WebSocket-Version: 13\r\n"+
		"Origin: https://www.samknows.com\r\n"+
		"User-Agent: %s\r\n\r\n",
		wsPath, host, key, ua)

	if _, err = tlsConn.Write([]byte(handshake)); err != nil {
		tlsConn.Close()
		return nil, fmt.Errorf("ws handshake write: %w", err)
	}

	// Read until we see the full HTTP response (\r\n\r\n)
	var respBuf []byte
	tmp := make([]byte, 4096)
	for {
		n, err := tlsConn.Read(tmp)
		if err != nil {
			tlsConn.Close()
			return nil, fmt.Errorf("ws handshake read: %w", err)
		}
		respBuf = append(respBuf, tmp[:n]...)
		if strings.Contains(string(respBuf), "\r\n\r\n") {
			break
		}
	}

	if !strings.Contains(string(respBuf), "101") {
		tlsConn.Close()
		return nil, fmt.Errorf("ws handshake: %s", string(respBuf[:min(200, len(respBuf))]))
	}

	return &wsConn{conn: tlsConn}, nil
}

func (ws *wsConn) sendJSON(v any) error {
	data, _ := json.Marshal(v)
	return ws.sendText(string(data))
}

func (ws *wsConn) sendText(payload string) error {
	var f []byte
	f = append(f, 0x81) // FIN + text
	length := len(payload)
	if length < 126 {
		f = append(f, byte(0x80|length))
	} else if length < 65536 {
		f = append(f, 0x80|126, byte(length>>8), byte(length))
	} else {
		f = append(f, 0x80|127)
		for i := 7; i >= 0; i-- {
			f = append(f, byte(length>>(8*i)))
		}
	}
	mask := make([]byte, 4)
	rand.Read(mask)
	f = append(f, mask...)
	for i := 0; i < length; i++ {
		f = append(f, payload[i]^mask[i%4])
	}
	_, err := ws.conn.Write(f)
	return err
}

func (ws *wsConn) recvFrame(timeout time.Duration) string {
	ws.conn.SetReadDeadline(time.Now().Add(timeout))
	defer ws.conn.SetReadDeadline(time.Time{})

	hdr := make([]byte, 2)
	if _, err := io.ReadFull(ws.conn, hdr); err != nil {
		return ""
	}

	length := int(hdr[1] & 0x7F)
	if length == 126 {
		ext := make([]byte, 2)
		if _, err := io.ReadFull(ws.conn, ext); err != nil {
			return ""
		}
		length = int(ext[0])<<8 | int(ext[1])
	} else if length == 127 {
		ext := make([]byte, 8)
		if _, err := io.ReadFull(ws.conn, ext); err != nil {
			return ""
		}
		for i := 0; i < 8; i++ {
			length = length<<8 | int(ext[i])
		}
	}

	buf := make([]byte, length)
	if _, err := io.ReadFull(ws.conn, buf); err != nil {
		return ""
	}

	opcode := hdr[0] & 0x0F
	if opcode == 9 {
		ws.conn.Write([]byte{0x8A, 0x00})
		return ""
	}
	if opcode == 8 {
		return ""
	}

	return string(buf)
}

func (ws *wsConn) close() {
	if ws.conn != nil {
		ws.conn.Close()
	}
}

// ---------------------------------------------------------------------------
// Hub discovery + login
// ---------------------------------------------------------------------------

type HubInfo struct {
	UnitID int
	Base   string
	Token  string
}

type Action struct {
	ID   int    `json:"id"`
	Task string `json:"task"`
}

func discoverHub() (*HubInfo, error) {
	dbg("discover hub...")
	for _, base := range []string{apiBase, apiBase2} {
		raw, err := httpGet(base + "/ip/discovery")
		if err != nil {
			continue
		}
		var resp map[string]any
		json.Unmarshal(raw, &resp)
		rawData := resp["data"]
		if rawData == nil {
			continue
		}
		dBytes, _ := json.Marshal(rawData)
		var units []map[string]any
		json.Unmarshal(dBytes, &units)
		if len(units) > 0 {
			uid := 0
			switch v := units[0]["id"].(type) {
			case float64:
				uid = int(v)
			case json.Number:
				n, _ := v.Int64()
				uid = int(n)
			}
			baseName, _ := units[0]["base"].(string)
			if uid > 0 {
				dbg("  unit_id=%d base=%s", uid, baseName)
				return &HubInfo{UnitID: uid, Base: baseName}, nil
			}
		}
	}
	return nil, fmt.Errorf("no hub found")
}

func loginHub(hub *HubInfo) error {
	dbg("login...")
	raw, err := httpPost(apiBase2+"/ip/login", map[string]int{"unit_id": hub.UnitID})
	if err != nil {
		return fmt.Errorf("login http: %w", err)
	}
	// Token is in data.accessToken or data.data.accessToken
	var resp map[string]any
	json.Unmarshal(raw, &resp)
	d, _ := resp["data"].(map[string]any)
	if d == nil {
		return fmt.Errorf("login: no data field")
	}
	token, _ := d["accessToken"].(string)
	if token == "" {
		dd, _ := d["data"].(map[string]any)
		if dd != nil {
			token, _ = dd["accessToken"].(string)
		}
	}
	if token == "" {
		return fmt.Errorf("login: no token")
	}
	hub.Token = token
	dbg("  token ok")
	return nil
}

// ---------------------------------------------------------------------------
// Router test (WebSocket protocol)
// ---------------------------------------------------------------------------

type RouterResult struct {
	DL, UL, Ping float64
}

func runRouterTest(hub *HubInfo) RouterResult {
	r := RouterResult{DL: -1, UL: -1, Ping: -1}
	dbg("== Router ==")

	ws1, err := wsConnect(wsHost, wsPort, "/trigger-testing", hub.Token)
	if err != nil {
		dbg("  ws connect: %v", err)
		return r
	}

	ws1.sendJSON(map[string]any{"type": "subscribe", "data": map[string]any{"unit_id": hub.UnitID}})
	resp := ws1.recvFrame(5 * time.Second)
	var sub map[string]any
	json.Unmarshal([]byte(resp), &sub)
	if sub == nil || !sub["success"].(bool) {
		dbg("  subscribe failed")
		ws1.close()
		return r
	}
	dbg("  subscribed")

	ws1.sendJSON(map[string]any{"type": "status", "data": map[string]any{"unit_id": hub.UnitID}})
	resp = ws1.recvFrame(5 * time.Second)
	var statusResp map[string]any
	json.Unmarshal([]byte(resp), &statusResp)
	actionsRaw, _ := statusResp["data"].(map[string]any)
	actionsArr, _ := actionsRaw["actions"].([]any)
	ws1.close()

	var actions []Action
	ab, _ := json.Marshal(actionsArr)
	json.Unmarshal(ab, &actions)
	dbg("  actions: %v", actions)
	if len(actions) == 0 {
		dbg("  no actions")
		return r
	}

	// Run download first, wait for it to complete, then run upload
	for i, wantTask := range []string{"http_get_mt", "http_post_mt"} {
		val := runOneRouterTest(hub, wantTask, actions)
		if val != nil {
			if strings.Contains(wantTask, "get") {
				r.DL = *val
			} else {
				r.UL = *val
			}
		}
		// Wait for the hub to fully complete download before starting upload
		if i == 0 {
			time.Sleep(3 * time.Second)
		}
	}

	return r
}

func runOneRouterTest(hub *HubInfo, wantTask string, actions []Action) *float64 {
	var actID int
	found := false
	for _, a := range actions {
		if a.Task == wantTask {
			actID = a.ID
			found = true
			break
		}
	}
	if !found {
		return nil
	}

	ws, err := wsConnect(wsHost, wsPort, "/trigger-testing", hub.Token)
	if err != nil {
		dbg("  ws reconnect: %v", err)
		return nil
	}
	defer ws.close()

	ws.sendJSON(map[string]any{"type": "subscribe", "data": map[string]any{"unit_id": hub.UnitID}})
	ws.recvFrame(5 * time.Second)

	ws.sendJSON(map[string]any{"type": "test", "data": map[string]any{"unit_id": hub.UnitID, "action_id": actID}})
	dbg("  trigger %s (id=%d)", wantTask, actID)

	var result *float64
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		raw := ws.recvFrame(30 * time.Second)
		if raw == "" {
			break
		}
		var d map[string]any
		if json.Unmarshal([]byte(raw), &d) != nil {
			continue
		}
		if d["type"] != "test" || !d["success"].(bool) {
			continue
		}
		td, _ := d["data"].(map[string]any)
		st, _ := td["status"].(string)
		tdData, _ := td["data"].(map[string]any)

		if st == "in_progress" {
			if bps, ok := tdData["bytes_sec"].(float64); ok {
				v := bps * 8 / 1e6
				result = &v
				dbg("  %s: %.0f Mbps", wantTask, v)
			}
		}
		if st == "finished" {
			if bps, ok := tdData["bytes_sec"].(float64); ok {
				v := bps * 8 / 1e6
				result = &v
				dbg("  finished %s: %.0f Mbps", wantTask, v)
			}
			break
		}
	}
	return result
}

// ---------------------------------------------------------------------------
// Device test (HTTP-based, concurrent goroutines — matches Python impl)
// ---------------------------------------------------------------------------

type DeviceResult struct {
	DL, UL, Ping float64
}

func runDeviceTest(durationSec int, concurrency int, pingProbes int) DeviceResult {
	r := DeviceResult{DL: -1, UL: -1, Ping: -1}
	dbg("== Device ==")

	// Find server
	raw, err := httpGet(targetsAPI)
	if err != nil {
		dbg("  targets: %v", err)
		return r
	}
	var servers []map[string]any
	json.Unmarshal(raw, &servers)
	if len(servers) == 0 {
		dbg("  no servers")
		return r
	}
	host, _ := servers[0]["hostname"].(string)
	if host == "" {
		dbg("  empty hostname")
		return r
	}
	dbg("  server: %s", host)

	// Run ping, download, upload concurrently (matches Python asyncio.gather)
	var wg sync.WaitGroup

	// Ping
	wg.Add(1)
	go func() {
		defer wg.Done()
		url := fmt.Sprintf("http://%s:%d/", host, dlPort)
		var lats []float64
		for i := 0; i < pingProbes; i++ {
			req, _ := http.NewRequest("GET", url, nil)
			req.Header.Set("User-Agent", ua)
			t0 := time.Now()
			resp, err := client.Do(req)
			if err == nil {
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				lats = append(lats, float64(time.Since(t0).Microseconds())/1000)
			}
		}
		if len(lats) > 0 {
			sort.Float64s(lats)
			if len(lats) > 2 {
				lats = lats[1 : len(lats)-1]
			}
			sum := 0.0
			for _, l := range lats {
				sum += l
			}
			r.Ping = sum / float64(len(lats))
		}
		dbg("  ping: %.0fms", r.Ping)
	}()

	// Download
	wg.Add(1)
	go func() {
		defer wg.Done()
		url := fmt.Sprintf("http://%s:%d/download", host, dlPort)
		var total atomic.Int64
		var dwg sync.WaitGroup
		t0 := time.Now()
		end := t0.Add(time.Duration(durationSec) * time.Second)
		for i := 0; i < concurrency; i++ {
			dwg.Add(1)
			go func() {
				defer dwg.Done()
				for time.Now().Before(end) {
					req, _ := http.NewRequest("GET", url, nil)
					req.Header.Set("User-Agent", ua)
					req.Header.Set("Accept-Encoding", "identity")
					req.Header.Set("Cache-Control", "no-cache")
					resp, err := client.Do(req)
					if err != nil {
						break
					}
					if resp.StatusCode != 200 {
						resp.Body.Close()
						break
					}
					buf := make([]byte, 64*1024)
					for {
						n, err := resp.Body.Read(buf)
						if err != nil {
							break
						}
						if time.Now().After(end) {
							resp.Body.Close()
							return
						}
						total.Add(int64(n))
					}
					resp.Body.Close()
				}
			}()
		}
		dwg.Wait()
		elapsed := time.Since(t0).Seconds()
		if elapsed < 0.001 {
			elapsed = 0.001
		}
		r.DL = float64(total.Load()) * 8 / elapsed / 1e6
		dbg("  dl: %.0f Mbps", r.DL)
	}()

	// Upload
	wg.Add(1)
	go func() {
		defer wg.Done()
		url := fmt.Sprintf("http://%s:%d/upload", host, ulPort)
		payload := make([]byte, 64*1024)
		rand.Read(payload)
		var total atomic.Int64
		var uwg sync.WaitGroup
		t0 := time.Now()
		end := t0.Add(time.Duration(durationSec) * time.Second)
		for i := 0; i < concurrency; i++ {
			uwg.Add(1)
			go func() {
				defer uwg.Done()
				for time.Now().Before(end) {
					req, _ := http.NewRequest("POST", url, strings.NewReader(string(payload)))
					req.Header.Set("User-Agent", ua)
					req.Header.Set("Content-Type", "application/octet-stream")
					resp, err := client.Do(req)
					if err != nil {
						break
					}
					io.Copy(io.Discard, resp.Body)
					resp.Body.Close()
					total.Add(int64(len(payload)))
					if time.Now().After(end) {
						return
					}
				}
			}()
		}
		uwg.Wait()
		elapsed := time.Since(t0).Seconds()
		if elapsed < 0.001 {
			elapsed = 0.001
		}
		r.UL = float64(total.Load()) * 8 / elapsed / 1e6
		dbg("  ul: %.0f Mbps", r.UL)
	}()

	wg.Wait()
	return r
}

// ---------------------------------------------------------------------------
// Output formatting
// ---------------------------------------------------------------------------

func fmtSpeed(v float64) string {
	if v < 0 {
		return "N/A"
	}
	if v >= 100 {
		return fmt.Sprintf("%.0f", v)
	}
	return fmt.Sprintf("%.1f", v)
}

func fmtPing(v float64) string {
	if v < 0 {
		return "N/A"
	}
	return fmt.Sprintf("%.0f", v)
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

func main() {
	fullFlag := flag.Bool("full", false, "Full test: router + device (~30s)")
	routerFlag := flag.Bool("router", false, "Router speed only")
	deviceFlag := flag.Bool("device", false, "Device speed only")
	jsonFlag := flag.Bool("json", false, "Output JSON")
	debugFlag := flag.Bool("debug", false, "Debug output on stderr")
	flag.Parse()
	debug = *debugFlag

	runRouter := true
	runDevice := true
	if *routerFlag {
		runDevice = false
	}
	if *deviceFlag {
		runRouter = false
	}

	dur := 3
	conc := 4
	probes := 5
	if *fullFlag {
		dur = 5
		conc = 6
		probes = 7
	}

	// Discover hub
	var hub *HubInfo
	if runRouter {
		h, err := discoverHub()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Hub: %v\n", err)
		} else {
			hub = h
			if err := loginHub(hub); err != nil {
				fmt.Fprintf(os.Stderr, "Login: %v\n", err)
				hub = nil
			}
		}
	}

	var router RouterResult
	var device DeviceResult

	if runRouter && runDevice && hub != nil {
		// Sequential: router first, then device
		router = runRouterTest(hub)
		device = runDeviceTest(dur, conc, probes)
	} else if runRouter && hub != nil {
		router = runRouterTest(hub)
	} else {
		device = runDeviceTest(dur, conc, probes)
	}

	// Output
	if *jsonFlag {
		nj := func(v float64) any {
			if v < 0 {
				return nil
			}
			return v
		}
		b, _ := json.Marshal(map[string]any{
			"router_dl_mbps":      nj(router.DL),
			"router_ul_mbps":      nj(router.UL),
			"device_dl_mbps":      nj(device.DL),
			"device_ul_mbps":      nj(device.UL),
			"ping_ms":             nj(device.Ping),
		})
		fmt.Println(string(b))
	} else {
		rDl := fmtSpeed(router.DL)
		rUl := fmtSpeed(router.UL)
		dDl := fmtSpeed(device.DL)
		dUl := fmtSpeed(device.UL)
		ping := fmtPing(device.Ping)

		if router.DL >= 0 || router.UL >= 0 {
			fmt.Printf("Router: %s/%s Mbps | Device: %s/%s Mbps | Ping: %s ms\n",
				rDl, rUl, dDl, dUl, ping)
		} else {
			fmt.Printf("Download: %s Mbps  Upload: %s Mbps  Ping: %s ms\n",
				dDl, dUl, ping)
		}
	}
}
