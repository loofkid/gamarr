package download

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"gamarr/internal/config"
	"gamarr/internal/qbit"
)

// TransmissionClient wraps the Transmission RPC API.
type TransmissionClient struct {
	cfg       *config.Config
	client    *http.Client
	mu        sync.Mutex
	sessionID string
}

const transmissionClientName = "Transmission"
const transmissionManagedLabel = "ROMs"

// NewTransmissionClient creates a new Transmission RPC client.
func NewTransmissionClient(cfg *config.Config) *TransmissionClient {
	return &TransmissionClient{
		cfg: cfg,
		client: &http.Client{
			Timeout: 15 * time.Second,
		},
	}
}

// transmissionRequest is the RPC request format.
type transmissionRequest struct {
	Method    string                 `json:"method"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// transmissionResponse is the RPC response format.
type transmissionResponse struct {
	Result    string                 `json:"result"`
	Arguments map[string]interface{} `json:"arguments,omitempty"`
}

// GetDownloadTorrents returns Transmission torrents in the shared format used
// by Gamarr's import and completion paths.
func (t *TransmissionClient) GetDownloadTorrents() ([]qbit.Torrent, error) {
	fields := []string{"id", "name", "status", "percentDone", "totalSize", "rateDownload", "eta", "hashString", "downloadDir"}
	items, err := t.GetTorrents(append(append([]string{}, fields...), "labels"))
	if err != nil {
		// Labels were added to Transmission's RPC after older supported releases.
		// Keep tracked-job polling functional when the field is unavailable; the
		// orphan watcher will simply have no ownership marker to match.
		slog.Debug("Transmission label field unavailable; retrying without labels", "error", err)
		items, err = t.GetTorrents(fields)
	}
	if err != nil {
		return nil, err
	}
	torrents := make([]qbit.Torrent, 0, len(items))
	for _, item := range items {
		name := transmissionString(item, "name")
		downloadDir := transmissionString(item, "downloadDir", "download_dir")
		contentPath := ""
		if downloadDir != "" && name != "" {
			contentPath = filepath.Join(downloadDir, filepath.FromSlash(name))
		}
		labels := transmissionStringSlice(item, "labels")
		torrents = append(torrents, qbit.Torrent{
			Name:        name,
			Hash:        strings.ToLower(transmissionString(item, "hashString", "hash_string")),
			Progress:    transmissionFloat(item, "percentDone", "percent_done"),
			State:       transmissionState(transmissionFloat(item, "status")),
			TotalSize:   int64(transmissionFloat(item, "totalSize", "total_size")),
			DLSpeed:     int64(transmissionFloat(item, "rateDownload", "rate_download")),
			ETA:         int(transmissionFloat(item, "eta")),
			SavePath:    downloadDir,
			ContentPath: contentPath,
			Client:      transmissionClientName,
			Labels:      labels,
		})
	}
	return torrents, nil
}

// GetTorrentFiles returns Transmission's ordered file list and per-file
// completion state for one torrent. Hash strings are stable across daemon
// restarts, unlike Transmission's integer IDs.
func (t *TransmissionClient) GetTorrentFiles(hash string) ([]qbit.TorrentFile, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	items, err := t.getTorrents([]interface{}{hash}, []string{"hashString", "files", "fileStats"})
	if err != nil {
		return nil, err
	}
	for _, item := range items {
		if got := transmissionString(item, "hashString", "hash_string"); !strings.EqualFold(got, hash) {
			continue
		}
		fileItems, _ := item["files"].([]interface{})
		statsItems, _ := item["fileStats"].([]interface{})
		if statsItems == nil {
			statsItems, _ = item["file_stats"].([]interface{})
		}
		files := make([]qbit.TorrentFile, 0, len(fileItems))
		for i, raw := range fileItems {
			file, ok := raw.(map[string]interface{})
			if !ok {
				continue
			}
			length := int64(transmissionFloat(file, "length"))
			completed := transmissionFloat(file, "bytesCompleted", "bytes_completed")
			if i < len(statsItems) {
				if stats, ok := statsItems[i].(map[string]interface{}); ok {
					completed = transmissionFloat(stats, "bytesCompleted", "bytes_completed")
				}
			}
			progress := 1.0
			if length > 0 {
				progress = completed / float64(length)
			}
			priority := 1
			if i < len(statsItems) {
				if stats, ok := statsItems[i].(map[string]interface{}); ok {
					if wanted, exists := stats["wanted"]; exists {
						if !transmissionBool(wanted) {
							priority = 0
						}
					}
				}
			}
			files = append(files, qbit.TorrentFile{
				Name:     transmissionString(file, "name"),
				Size:     length,
				Progress: progress,
				Priority: priority,
				Index:    i,
			})
		}
		return files, nil
	}
	return nil, nil
}

func transmissionString(item map[string]interface{}, keys ...string) string {
	for _, key := range keys {
		if value, ok := item[key].(string); ok {
			return value
		}
	}
	return ""
}

func transmissionStringSlice(item map[string]interface{}, keys ...string) []string {
	for _, key := range keys {
		values, ok := item[key].([]interface{})
		if !ok {
			continue
		}
		out := make([]string, 0, len(values))
		for _, value := range values {
			if text, ok := value.(string); ok {
				out = append(out, text)
			}
		}
		return out
	}
	return nil
}

func transmissionFloat(item map[string]interface{}, keys ...string) float64 {
	for _, key := range keys {
		if value, ok := item[key].(float64); ok {
			return value
		}
	}
	return 0
}

func transmissionBool(value interface{}) bool {
	switch v := value.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case int:
		return v != 0
	default:
		return false
	}
}

func transmissionState(status float64) string {
	switch int(status) {
	case 0:
		return "stopped"
	case 1:
		return "queuedVerify"
	case 2:
		return "checking"
	case 3:
		return "queuedDL"
	case 4:
		return "downloading"
	case 5:
		return "queuedUP"
	case 6:
		return "seeding"
	default:
		return "unknown"
	}
}

// AddTorrent adds a torrent by URL or magnet link.
func (t *TransmissionClient) AddTorrent(url, downloadDir string) (map[string]interface{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	args := map[string]interface{}{
		"filename": url,
		"labels":   []string{transmissionManagedLabel},
	}
	if downloadDir != "" {
		args["download-dir"] = downloadDir
	}

	resp, err := t.call("torrent-add", args)
	if err != nil || resp.Result != "success" {
		// torrent-add labels are newer than the base RPC, so retry without the
		// optional ownership marker on servers that reject that argument.
		delete(args, "labels")
		resp, err = t.call("torrent-add", args)
		if err != nil {
			return nil, err
		}
		if resp.Result != "success" {
			return nil, fmt.Errorf("transmission: %s", resp.Result)
		}
	}

	// The response may have "torrent-added" or "torrent-duplicate".
	if added, ok := resp.Arguments["torrent-added"]; ok {
		if m, ok := added.(map[string]interface{}); ok {
			return m, nil
		}
	}
	if dup, ok := resp.Arguments["torrent-duplicate"]; ok {
		if m, ok := dup.(map[string]interface{}); ok {
			return m, nil
		}
	}

	return resp.Arguments, nil
}

// GetTorrents returns status info for all torrents.
// If fields is nil, returns a default set of fields.
func (t *TransmissionClient) GetTorrents(fields []string) ([]map[string]interface{}, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if fields == nil {
		fields = []string{"id", "name", "status", "percentDone", "totalSize", "rateDownload", "hashString", "downloadDir"}
	}
	return t.getTorrents(nil, fields)
}

// getTorrents requests a Transmission torrent subset. The caller holds t.mu.
func (t *TransmissionClient) getTorrents(ids interface{}, fields []string) ([]map[string]interface{}, error) {

	args := map[string]interface{}{
		"fields": fields,
	}
	if ids != nil {
		args["ids"] = ids
	}

	resp, err := t.call("torrent-get", args)
	if err != nil {
		return nil, err
	}
	if resp.Result != "success" {
		return nil, fmt.Errorf("transmission: %s", resp.Result)
	}

	torrentsRaw, ok := resp.Arguments["torrents"]
	if !ok {
		return nil, nil
	}

	// Convert to []map[string]interface{}.
	torrentsJSON, err := json.Marshal(torrentsRaw)
	if err != nil {
		return nil, err
	}
	var torrents []map[string]interface{}
	if err := json.Unmarshal(torrentsJSON, &torrents); err != nil {
		return nil, err
	}
	return torrents, nil
}

// RemoveTorrent removes torrents by ID.
func (t *TransmissionClient) RemoveTorrent(ids []int, deleteData bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	values := make([]interface{}, len(ids))
	for i, id := range ids {
		values[i] = id
	}
	return t.removeTorrents(values, deleteData)
}

// RemoveTorrentByHash removes one torrent while keeping or deleting its data.
func (t *TransmissionClient) RemoveTorrentByHash(hash string, deleteData bool) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.removeTorrents([]interface{}{hash}, deleteData)
}

func (t *TransmissionClient) removeTorrents(ids []interface{}, deleteData bool) error {
	resp, err := t.call("torrent-remove", map[string]interface{}{
		"ids":               ids,
		"delete-local-data": deleteData,
	})
	if err != nil {
		return err
	}
	if resp.Result != "success" {
		return fmt.Errorf("transmission: %s", resp.Result)
	}
	return nil
}

// StopTorrent halts one torrent but leaves both its entry and files intact.
func (t *TransmissionClient) StopTorrent(hash string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.torrentAction("torrent-stop", hash)
}

// StartTorrent resumes one torrent, using its stable hash as the RPC identifier.
func (t *TransmissionClient) StartTorrent(hash string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.torrentAction("torrent-start", hash)
}

// SetFileSelection marks the requested file indexes wanted and the rest
// unwanted for one torrent.
func (t *TransmissionClient) SetFileSelection(hash string, keep, skip []int) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	args := map[string]interface{}{"ids": []interface{}{hash}}
	if len(keep) > 0 {
		args["files-wanted"] = keep
	}
	if len(skip) > 0 {
		args["files-unwanted"] = skip
	}
	resp, err := t.call("torrent-set", args)
	if err != nil {
		return err
	}
	if resp.Result != "success" {
		return fmt.Errorf("transmission: %s", resp.Result)
	}
	return nil
}

func (t *TransmissionClient) torrentAction(method, hash string) error {
	resp, err := t.call(method, map[string]interface{}{"ids": []interface{}{hash}})
	if err != nil {
		return err
	}
	if resp.Result != "success" {
		return fmt.Errorf("transmission: %s", resp.Result)
	}
	return nil
}

// Diagnose tests the Transmission connection.
func (t *TransmissionClient) Diagnose() map[string]interface{} {
	t.mu.Lock()
	defer t.mu.Unlock()

	resp, err := t.call("session-get", nil)
	if err != nil {
		return map[string]interface{}{
			"success": false,
			"error":   err.Error(),
		}
	}
	if resp.Result != "success" {
		return map[string]interface{}{
			"success": false,
			"error":   resp.Result,
		}
	}
	return map[string]interface{}{
		"success": true,
	}
}

func (t *TransmissionClient) call(method string, args map[string]interface{}) (*transmissionResponse, error) {
	reqBody := transmissionRequest{
		Method:    method,
		Arguments: args,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return nil, err
	}

	url := fmt.Sprintf("%s/transmission/rpc", t.cfg.TransmissionURL)
	resp, err := t.doRequest(url, body)
	if err != nil {
		return nil, err
	}

	// Handle 409 Conflict — Transmission requires a session-id header.
	if resp.StatusCode == http.StatusConflict {
		t.sessionID = resp.Header.Get("X-Transmission-Session-Id")
		resp.Body.Close()
		if t.sessionID == "" {
			return nil, fmt.Errorf("transmission: 409 but no session ID header")
		}
		resp, err = t.doRequest(url, body)
		if err != nil {
			return nil, err
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		return nil, fmt.Errorf("transmission: authentication failed")
	}

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var trResp transmissionResponse
	if err := json.Unmarshal(respBody, &trResp); err != nil {
		return nil, fmt.Errorf("transmission: invalid JSON response")
	}

	return &trResp, nil
}

func (t *TransmissionClient) doRequest(url string, body []byte) (*http.Response, error) {
	req, err := http.NewRequest("POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	if t.sessionID != "" {
		req.Header.Set("X-Transmission-Session-Id", t.sessionID)
	}
	if t.cfg.TransmissionUser != "" {
		req.SetBasicAuth(t.cfg.TransmissionUser, t.cfg.TransmissionPass)
	}
	return t.client.Do(req)
}
