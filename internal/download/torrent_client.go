package download

import (
	"fmt"
	"log/slog"
	"strings"

	"gamarr/internal/qbit"
)

const qbittorrentClientName = "qBittorrent"

// IsGamarrManagedTransmissionTorrent reports whether Transmission marked a
// torrent as submitted by Gamarr.
func IsGamarrManagedTransmissionTorrent(t qbit.Torrent) bool {
	if !strings.EqualFold(t.Client, transmissionClientName) {
		return false
	}
	for _, label := range t.Labels {
		if strings.EqualFold(label, transmissionManagedLabel) {
			return true
		}
	}
	return false
}

// torrentsForClient returns normalized torrent entries for the requested
// backend and annotates them so later import operations return to that client.
func (m *Manager) torrentsForClient(clientName string) ([]qbit.Torrent, error) {
	if strings.EqualFold(clientName, transmissionClientName) {
		if m.transmission == nil {
			return nil, fmt.Errorf("Transmission is not configured")
		}
		return m.transmission.GetDownloadTorrents()
	}
	if m.qb == nil {
		return nil, fmt.Errorf("qBittorrent is not configured")
	}
	torrents, err := m.qb.GetTorrents(m.cfg.QBCategory)
	if err != nil {
		return nil, err
	}
	for i := range torrents {
		torrents[i].Client = qbittorrentClientName
	}
	return torrents, nil
}

// ConfiguredTorrents returns torrents from each configured client.
func (m *Manager) ConfiguredTorrents() []qbit.Torrent {
	var out []qbit.Torrent
	for _, clientName := range []string{qbittorrentClientName, transmissionClientName} {
		if strings.EqualFold(clientName, qbittorrentClientName) && !m.cfg.HasQBittorrent() {
			continue
		}
		if strings.EqualFold(clientName, transmissionClientName) && !m.cfg.HasTransmission() {
			continue
		}
		torrents, err := m.torrentsForClient(clientName)
		if err != nil {
			slog.Warn("could not read torrents from a configured download client", "client", clientName, "error", err)
			continue
		}
		out = append(out, torrents...)
	}
	return out
}

// TorrentFiles returns per-file state from the client that owns the torrent.
func (m *Manager) TorrentFiles(t qbit.Torrent) []qbit.TorrentFile {
	return m.torrentFiles(t)
}

// DeleteTorrentByHash removes the matching torrent and its data from the
// preferred backend when known, otherwise searching configured clients in
// their historical order.
func (m *Manager) DeleteTorrentByHash(hash, preferredClient string, deleteData bool) bool {
	if strings.EqualFold(preferredClient, transmissionClientName) || strings.EqualFold(preferredClient, qbittorrentClientName) {
		if strings.EqualFold(preferredClient, transmissionClientName) && !m.cfg.HasTransmission() {
			return false
		}
		if strings.EqualFold(preferredClient, qbittorrentClientName) && !m.cfg.HasQBittorrent() {
			return false
		}
		torrents, err := m.torrentsForClient(preferredClient)
		if err != nil {
			slog.Warn("could not read preferred download client for torrent removal", "client", preferredClient, "error", err)
			return false
		}
		for _, torrent := range torrents {
			if strings.EqualFold(torrent.Hash, hash) {
				return m.removeTorrent(torrent, deleteData)
			}
		}
		return false
	}

	clients := make([]string, 0, 2)
	for _, clientName := range []string{qbittorrentClientName, transmissionClientName} {
		clients = append(clients, clientName)
	}

	for _, clientName := range clients {
		if strings.EqualFold(clientName, qbittorrentClientName) && !m.cfg.HasQBittorrent() {
			continue
		}
		if strings.EqualFold(clientName, transmissionClientName) && !m.cfg.HasTransmission() {
			continue
		}
		torrents, err := m.torrentsForClient(clientName)
		if err != nil {
			continue
		}
		for _, torrent := range torrents {
			if strings.EqualFold(torrent.Hash, hash) {
				return m.removeTorrent(torrent, deleteData)
			}
		}
	}
	return false
}

// torrentFiles returns per-file state using the backend that owns this torrent.
func (m *Manager) torrentFiles(t qbit.Torrent) []qbit.TorrentFile {
	if strings.EqualFold(t.Client, transmissionClientName) {
		if m.transmission == nil {
			return nil
		}
		files, err := m.transmission.GetTorrentFiles(t.Hash)
		if err != nil {
			slog.Warn("could not read Transmission file list", "hash", t.Hash, "error", err)
			return nil
		}
		return files
	}
	if m.qb == nil {
		return nil
	}
	return m.qb.GetTorrentFiles(t.Hash)
}

func (m *Manager) stopTorrent(t qbit.Torrent) bool {
	if strings.EqualFold(t.Client, transmissionClientName) {
		if m.transmission == nil {
			return false
		}
		if err := m.transmission.StopTorrent(t.Hash); err != nil {
			slog.Warn("could not stop Transmission torrent", "hash", t.Hash, "error", err)
			return false
		}
		return true
	}
	return m.qb != nil && m.qb.StopTorrent(t.Hash)
}

func (m *Manager) removeTorrent(t qbit.Torrent, deleteData bool) bool {
	if strings.EqualFold(t.Client, transmissionClientName) {
		if m.transmission == nil {
			return false
		}
		if err := m.transmission.RemoveTorrentByHash(t.Hash, deleteData); err != nil {
			slog.Warn("could not remove Transmission torrent", "hash", t.Hash, "error", err)
			return false
		}
		return true
	}
	return m.qb != nil && m.qb.DeleteTorrent(t.Hash, deleteData)
}

func torrentClientFromJob(data map[string]interface{}) string {
	if client, ok := data["download_client"].(string); ok && strings.TrimSpace(client) != "" {
		return client
	}
	// Persisted jobs from earlier Gamarr versions only came from qBittorrent.
	return qbittorrentClientName
}

func torrentClientForTorrent(t qbit.Torrent) string {
	if strings.TrimSpace(t.Client) != "" {
		return t.Client
	}
	// Torrent fixtures and persisted qBittorrent jobs predate the Client field.
	return qbittorrentClientName
}

// torrentHashClaim keeps legacy qBittorrent in-process keys stable while
// isolating the same infohash in another backend.
func torrentHashClaim(clientName, hash string) string {
	hash = strings.ToLower(strings.TrimSpace(hash))
	if hash == "" {
		return ""
	}
	if strings.EqualFold(clientName, transmissionClientName) {
		return "transmission:" + hash
	}
	return hash
}

func torrentImportClaim(t qbit.Torrent, jobID string, job map[string]interface{}) string {
	clientName := strings.TrimSpace(t.Client)
	if clientName == "" {
		clientName = torrentClientFromJob(job)
	}
	claim := torrentHashClaim(clientName, t.Hash)
	if claim == "" {
		return jobID
	}
	title, _ := job["title"].(string)
	if title != "" && !strings.EqualFold(title, t.Name) && !isGenericArchiveTorrentName(title) {
		return claim + ":" + jobID
	}
	return claim
}
