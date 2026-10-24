package search

import (
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"gamarr/internal/models"
	"gamarr/internal/platform"
)

const (
	npsCatalogBaseURL = "https://nopaystation.com/tsv/"
	npsCatalogTTL     = time.Hour
	npsCatalogMaxSize = 32 << 20
	npsMaxResults     = 20
)

var npsConsoles = map[string]string{
	"ps3":    "PS3",
	"psp":    "PSP",
	"psvita": "PSV",
	"psx":    "PSX",
}

type npsEntry struct {
	TitleID   string
	ContentID string
	Region    string
	Name      string
	URL       string
	Size      int64
	SHA256    string
}

type npsCatalog struct {
	entries   []npsEntry
	fetchedAt time.Time
	etag      string
	modified  string
}

var npsCatalogs = struct {
	sync.Mutex
	items   map[string]npsCatalog
	loading map[string]chan struct{}
}{items: make(map[string]npsCatalog), loading: make(map[string]chan struct{})}

// SearchNoPayStation reads NoPayStation's public game TSV catalogs.
// Only the four console catalogs represented by Gamarr's platform registry
// are queried. An empty platform searches each supported catalog.
func SearchNoPayStation(query, platformSlug string) []*models.SearchResult {
	query = strings.TrimSpace(query)
	if query == "" || IsCircuitOpen("nopaystation") {
		return nil
	}

	platformSlug = strings.ToLower(strings.TrimSpace(platformSlug))
	if platformSlug == "all" {
		platformSlug = ""
	}
	var consoles []string
	if platformSlug != "" {
		console, ok := npsConsoles[platformSlug]
		if !ok {
			return nil
		}
		consoles = []string{console}
	} else {
		consoles = []string{"PS3", "PSP", "PSV", "PSX"}
	}

	type catalogResult struct {
		console string
		entries []npsEntry
		err     error
	}
	resultsCh := make(chan catalogResult, len(consoles))
	for _, console := range consoles {
		go func(console string) {
			entries, err := loadNPSCatalog(console)
			resultsCh <- catalogResult{console: console, entries: entries, err: err}
		}(console)
	}

	var results []*models.SearchResult
	for range consoles {
		catalog := <-resultsCh
		if catalog.err != nil {
			slog.Warn("NoPayStation catalog refresh failed", "console", catalog.console, "error", catalog.err)
		}
		slug := npsSlug(catalog.console)
		p, ok := platform.Lookup(slug)
		if !ok {
			continue
		}
		type scoredEntry struct {
			entry npsEntry
			score int
		}
		var matches []scoredEntry
		for _, item := range catalog.entries {
			if score := npsEntryMatchScore(query, item); score > 0 {
				matches = append(matches, scoredEntry{entry: item, score: score})
			}
		}
		sort.SliceStable(matches, func(i, j int) bool { return matches[i].score > matches[j].score })
		if len(matches) > npsMaxResults {
			matches = matches[:npsMaxResults]
		}
		for _, match := range matches {
			item := match.entry
			title := item.Name
			if item.Region != "" && !strings.Contains(strings.ToLower(title), strings.ToLower(item.Region)) {
				title += " [" + item.Region + "]"
			}
			guid := item.TitleID
			if item.ContentID != "" {
				guid += ":" + item.ContentID
			} else {
				guid += ":" + item.Region + ":" + item.Name
			}
			sizeHuman := "?"
			if item.Size > 0 {
				sizeHuman = HumanSize(item.Size)
			}
			results = append(results, &models.SearchResult{
				Title:          title,
				Size:           item.Size,
				SizeHuman:      sizeHuman,
				Indexer:        "NoPayStation",
				DownloadURL:    item.URL,
				GUID:           "nopaystation:" + guid,
				Platform:       p.Name,
				PlatformSlug:   slug,
				IsPC:           p.IsPC,
				SourceType:     "ddl",
				SafetyScore:    95,
				SafetyWarnings: []string{},
				SHA256:         item.SHA256,
			})
		}
	}
	return results
}

func npsSlug(console string) string {
	for slug, name := range npsConsoles {
		if name == console {
			return slug
		}
	}
	return ""
}

func npsEntryMatchScore(query string, item npsEntry) int {
	q := strings.ToLower(strings.TrimSpace(query))
	titleID := strings.ToLower(item.TitleID)
	name := strings.ToLower(item.Name)
	if q == "" {
		return 0
	}
	if titleID == q {
		return 1000
	}
	if name == q {
		return 900
	}
	if len(q) >= 4 && strings.Contains(titleID, q) {
		return 850
	}
	if len(q) >= 3 && strings.Contains(name, q) {
		return 750
	}
	queryWords := extractWords(query)
	if len(queryWords) == 0 {
		return 0
	}
	nameWords := extractWords(item.Name)
	overlap := countOverlap(queryWords, nameWords)
	minimum := 1
	if len(queryWords) == 2 {
		minimum = 2
	} else if len(queryWords) > 2 {
		minimum = len(queryWords) - 1
	}
	if overlap < minimum {
		return 0
	}
	return 100*overlap/len(queryWords) + overlap
}

func loadNPSCatalog(console string) ([]npsEntry, error) {
	npsCatalogs.Lock()
	if cached, ok := npsCatalogs.items[console]; ok && time.Since(cached.fetchedAt) < npsCatalogTTL {
		npsCatalogs.Unlock()
		return cached.entries, nil
	}
	if loading, ok := npsCatalogs.loading[console]; ok {
		npsCatalogs.Unlock()
		<-loading
		npsCatalogs.Lock()
		cached := npsCatalogs.items[console]
		npsCatalogs.Unlock()
		if len(cached.entries) > 0 {
			return cached.entries, nil
		}
		return nil, fmt.Errorf("catalog fetch did not produce entries")
	}
	done := make(chan struct{})
	npsCatalogs.loading[console] = done
	previous := npsCatalogs.items[console]
	npsCatalogs.Unlock()

	fresh, etag, modified, err := fetchNPSCatalog(console, previous)
	npsCatalogs.Lock()
	if err == nil {
		if fresh == nil { // HTTP 304; retain the parsed rows.
			fresh = previous.entries
		}
		npsCatalogs.items[console] = npsCatalog{
			entries: fresh, fetchedAt: time.Now(), etag: etag, modified: modified,
		}
	}
	delete(npsCatalogs.loading, console)
	close(done)
	stale := previous.entries
	npsCatalogs.Unlock()

	if err != nil {
		RecordSearchFail("nopaystation", err.Error())
		return stale, err
	}
	RecordSearchSuccess("nopaystation")
	return fresh, nil
}

func fetchNPSCatalog(console string, previous npsCatalog) ([]npsEntry, string, string, error) {
	if _, ok := npsConsoles[npsSlug(console)]; !ok {
		return nil, "", "", fmt.Errorf("unsupported NoPayStation catalog %q", console)
	}
	url := npsCatalogBaseURL + console + "_GAMES.tsv"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, "", "", err
	}
	req.Header.Set("User-Agent", "Gamarr/1.0 (+https://github.com/JeremiahM37/gamarr)")
	if previous.etag != "" {
		req.Header.Set("If-None-Match", previous.etag)
	}
	if previous.modified != "" {
		req.Header.Set("If-Modified-Since", previous.modified)
	}

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotModified {
		etag := resp.Header.Get("ETag")
		if etag == "" {
			etag = previous.etag
		}
		modified := resp.Header.Get("Last-Modified")
		if modified == "" {
			modified = previous.modified
		}
		return nil, etag, modified, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", "", fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, npsCatalogMaxSize+1))
	if err != nil {
		return nil, "", "", err
	}
	if len(body) > npsCatalogMaxSize {
		return nil, "", "", fmt.Errorf("catalog exceeded %d MiB size limit", npsCatalogMaxSize>>20)
	}
	entries, err := parseNPSCatalog(body)
	if err != nil {
		return nil, "", "", err
	}
	return entries, resp.Header.Get("ETag"), resp.Header.Get("Last-Modified"), nil
}

func parseNPSCatalog(body []byte) ([]npsEntry, error) {
	reader := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(body), "\uFEFF")))
	reader.Comma = '\t'
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	columns, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("read catalog header: %w", err)
	}
	indices := make(map[string]int, len(columns))
	for i, column := range columns {
		indices[npsColumnKey(column)] = i
	}
	for _, required := range []string{"titleid", "name", "pkgdirectlink"} {
		if _, ok := indices[required]; !ok {
			return nil, fmt.Errorf("catalog missing %q column", required)
		}
	}
	value := func(row []string, key string) string {
		i, ok := indices[key]
		if !ok || i >= len(row) {
			return ""
		}
		return strings.TrimSpace(row[i])
	}

	var entries []npsEntry
	for {
		row, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("read catalog row: %w", err)
		}
		titleID := value(row, "titleid")
		name := value(row, "name")
		pkgURL := value(row, "pkgdirectlink")
		if titleID == "" || name == "" || !NoPayStationDownloadURLAllowed(pkgURL) {
			continue
		}
		checksum := strings.ToLower(value(row, "sha256"))
		if len(checksum) != 64 {
			checksum = ""
		} else if _, err := hex.DecodeString(checksum); err != nil {
			checksum = ""
		}
		size, _ := strconv.ParseInt(strings.ReplaceAll(value(row, "filesize"), ",", ""), 10, 64)
		entries = append(entries, npsEntry{
			TitleID:   titleID,
			ContentID: value(row, "contentid"),
			Region:    value(row, "region"),
			Name:      name,
			URL:       pkgURL,
			Size:      size,
			SHA256:    checksum,
		})
	}
	return entries, nil
}

func npsColumnKey(column string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(column)) {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// NoPayStationDownloadURLAllowed rejects non-package hosts and custom ports.
// Official catalog URLs use the Sony package hosts, including the explicit
// default HTTP port on a small number of rows.
func NoPayStationDownloadURLAllowed(raw string) bool {
	parsed, err := url.Parse(strings.TrimSpace(raw))
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.User != nil {
		return false
	}
	if port := parsed.Port(); port != "" && !((parsed.Scheme == "http" && port == "80") || (parsed.Scheme == "https" && port == "443")) {
		return false
	}
	host := strings.ToLower(strings.TrimSuffix(parsed.Hostname(), "."))
	return host == "dl.playstation.net" || strings.HasSuffix(host, ".dl.playstation.net")
}
