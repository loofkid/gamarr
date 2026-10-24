package download

import (
	"log/slog"
	"strings"
	"sync"
	"time"

	"gamarr/internal/config"
	"gamarr/internal/qbit"
)

// Watcher monitors configured torrent clients for completed downloads and
// auto-imports torrents that do not already have a tracked job.
type Watcher struct {
	cfg        *config.Config
	mgr        *Manager
	processing sync.Map // backend-scoped torrent key → struct{} (currently being processed)
	imported   sync.Map // backend-scoped torrent key → struct{} (already imported)
	stopCh     chan struct{}
}

// NewWatcher creates a torrent completion watcher.
func NewWatcher(cfg *config.Config, mgr *Manager) *Watcher {
	return &Watcher{
		cfg:    cfg,
		mgr:    mgr,
		stopCh: make(chan struct{}),
	}
}

// Start begins watching for completed torrents.
func (w *Watcher) Start() {
	if !w.cfg.WatcherEnabled || (!w.cfg.HasQBittorrent() && !w.cfg.HasTransmission()) {
		slog.Info("torrent watcher disabled")
		return
	}

	interval := time.Duration(w.cfg.WatcherIntervalS) * time.Second
	if interval < 10*time.Second {
		interval = 30 * time.Second
	}

	slog.Info("torrent watcher started", "interval", interval, "qBittorrent", w.cfg.HasQBittorrent(), "Transmission", w.cfg.HasTransmission())

	// Run immediately on start.
	w.checkCompleted()

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				w.checkCompleted()
			case <-w.stopCh:
				slog.Info("torrent watcher stopped")
				return
			}
		}
	}()
}

// Stop signals the watcher to stop.
func (w *Watcher) Stop() {
	select {
	case <-w.stopCh:
		// Already closed.
	default:
		close(w.stopCh)
	}
}

// checkCompleted scans qBittorrent for completed torrents that haven't been imported.
func (w *Watcher) checkCompleted() {
	clients := make([]string, 0, 2)
	if w.cfg.HasQBittorrent() {
		clients = append(clients, qbittorrentClientName)
	}
	if w.cfg.HasTransmission() {
		clients = append(clients, transmissionClientName)
	}
	for _, clientName := range clients {
		torrents, err := w.mgr.torrentsForClient(clientName)
		if err != nil {
			slog.Warn("watcher: could not read a download client, skipping it for this pass", "client", clientName, "error", err)
			continue
		}
		for _, t := range torrents {
			if t.Progress < 1.0 {
				continue
			}
			// qBittorrent is already scoped to Gamarr's category. Transmission has
			// no category filter here, so only auto-import torrents carrying the
			// label applied by Gamarr when it submits them.
			if strings.EqualFold(t.Client, transmissionClientName) && !IsGamarrManagedTransmissionTorrent(t) {
				continue
			}

			key := torrentHashClaim(torrentClientForTorrent(t), t.Hash)
			if key == "" {
				key = torrentHashClaim(torrentClientForTorrent(t), "title:"+t.Name)
			}
			// Skip if already imported or currently processing in this backend.
			if _, ok := w.imported.Load(key); ok {
				continue
			}
			if _, ok := w.processing.Load(key); ok {
				continue
			}

			// Skip if we already have a tracked job for this torrent.
			if w.hasMatchingJob(t) {
				continue
			}

			w.processing.Store(key, struct{}{})
			go w.importTorrentWithKey(t, key)
		}
	}
}

// hasMatchingJob checks if there's already a job tracking this torrent. It uses
// the same match as Manager.watchGameTorrent so a torrent renamed by the tracker
// (still claimed by an active job) is not double-imported.
func (w *Watcher) hasMatchingJob(t qbit.Torrent) bool {
	for _, item := range w.mgr.Jobs().Items() {
		if !strings.EqualFold(torrentClientFromJob(item.Data), torrentClientForTorrent(t)) {
			continue
		}
		title, _ := item.Data["title"].(string)
		infoHash, _ := item.Data["info_hash"].(string)
		if JobMatchesTorrent(infoHash, title, t.Hash, t.Name) {
			return true
		}
	}
	return false
}

// importTorrent auto-imports a completed torrent into the library.
func (w *Watcher) importTorrent(t qbit.Torrent) {
	key := torrentHashClaim(torrentClientForTorrent(t), t.Hash)
	if key == "" {
		key = torrentHashClaim(torrentClientForTorrent(t), "title:"+t.Name)
	}
	w.importTorrentWithKey(t, key)
}

func (w *Watcher) importTorrentWithKey(t qbit.Torrent, key string) {
	defer w.processing.Delete(key)

	slog.Info("watcher: auto-importing completed torrent", "name", t.Name, "hash", t.Hash)

	// Detect platform from existing library items or default to PC.
	platf := "PC"
	platSlug := "pc"
	isPC := true

	// Check if there's a library item with a matching title for platform hints.
	if existing := w.mgr.Jobs().FindLibraryByTitle(t.Name, ""); existing != nil {
		platf = existing.Platform
		platSlug = existing.PlatformSlug
		isPC = existing.IsPC
	}

	// Create a job and organize.
	jobID := newJobID()
	w.mgr.Jobs().Set(jobID, map[string]interface{}{
		"status":        "organizing",
		"title":         t.Name,
		"info_hash":     t.Hash,
		"download_client": t.Client,
		"platform":      platf,
		"platform_slug": platSlug,
		"is_pc":         isPC,
		"error":         nil,
		"detail":        "Auto-importing completed torrent...",
	})

	if w.mgr.importFinishedTorrent("watcher", jobID, t, platf, platSlug, isPC) {
		w.imported.Store(key, struct{}{})

		// Optionally remove the torrent after import.
		if w.cfg.RemoveAfterImport {
			w.mgr.removeTorrent(t, false)
			slog.Info("watcher: removed torrent after import", "name", t.Name)
		}

		// Send notification.
		if w.mgr.NotifyFunc != nil {
			w.mgr.NotifyFunc("", "download_complete", t.Name,
				"Auto-imported by watcher: "+t.Name+" ("+platf+")")
		}
		return
	}

	// Bounded rather than disabled. The guard this replaces was right that a
	// broken import must not spin forever, but it could not tell a transient miss
	// from a permanent one and so treated every failure as permanent.
	w.imported.Store(key, struct{}{})
}
