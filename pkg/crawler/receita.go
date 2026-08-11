package crawler

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"sort"
	"strings"
	"time"
)

var (
	monthRegex = regexp.MustCompile(`(\d{4}-\d{2})`)
	zipRegex   = regexp.MustCompile(`([a-zA-Z0-9_\-]+\.zip)`)
)

type ReceitaCrawler struct {
	BaseURL    string
	WebDAVURL  string
	Token      string
	HTTPClient *http.Client
}

func NewReceitaCrawler(baseURL string) *ReceitaCrawler {
	webDAVURL := baseURL
	token := "YggdBLfdninEJX9"

	if strings.Contains(baseURL, "/index.php/s/") {
		parts := strings.Split(baseURL, "/index.php/s/")
		if len(parts) > 1 {
			token = strings.Trim(parts[1], "/")
			webDAVURL = parts[0] + "/public.php/webdav/"
		}
	} else if strings.Contains(baseURL, "/public.php/webdav") {
		webDAVURL = baseURL
	} else if !strings.Contains(baseURL, "webdav") {
		webDAVURL = "https://arquivos.receitafederal.gov.br/public.php/webdav/"
	}

	if !strings.HasSuffix(webDAVURL, "/") {
		webDAVURL += "/"
	}

	return &ReceitaCrawler{
		BaseURL:   baseURL,
		WebDAVURL: webDAVURL,
		Token:     token,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

func (c *ReceitaCrawler) prepareRequest(method, reqURL string) (*http.Request, error) {
	req, err := http.NewRequest(method, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	if c.Token != "" {
		req.SetBasicAuth(c.Token, "")
	}
	if method == "PROPFIND" {
		req.Header.Set("Depth", "1")
	}
	return req, nil
}

// GetLatestMonth fetches the base directory HTML/XML and returns the latest YYYY-MM folder
func (c *ReceitaCrawler) GetLatestMonth() (string, error) {
	req, err := c.prepareRequest("PROPFIND", c.WebDAVURL)
	if err != nil {
		return "", fmt.Errorf("failed creating request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		req, err = c.prepareRequest("GET", c.BaseURL)
		if err != nil {
			return "", fmt.Errorf("failed creating fallback request: %w", err)
		}
		resp, err = c.HTTPClient.Do(req)
		if err != nil {
			return "", fmt.Errorf("failed to fetch base URL %s: %w", c.BaseURL, err)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", fmt.Errorf("HTTP error %d accessing base URL", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read response body: %w", err)
	}

	matches := monthRegex.FindAllStringSubmatch(string(body), -1)
	if len(matches) == 0 {
		return "", fmt.Errorf("no month directories found at %s", c.BaseURL)
	}

	monthsMap := make(map[string]bool)
	for _, m := range matches {
		if len(m) > 1 {
			monthsMap[m[1]] = true
		}
	}

	var months []string
	for m := range monthsMap {
		months = append(months, m)
	}
	sort.Strings(months)

	latest := months[len(months)-1]
	return latest, nil
}

// ListZipFiles fetches the HTML/XML for a month directory and returns list of .zip filenames
func (c *ReceitaCrawler) ListZipFiles(dataMonth string) (string, []string, error) {
	dataURL := fmt.Sprintf("%s/%s/", strings.TrimSuffix(c.WebDAVURL, "/"), dataMonth)

	req, err := c.prepareRequest("PROPFIND", dataURL)
	if err != nil {
		return "", nil, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := c.HTTPClient.Do(req)
	if err != nil || resp.StatusCode >= 400 {
		req, err = c.prepareRequest("GET", dataURL)
		if err != nil {
			return "", nil, fmt.Errorf("failed to create fallback request: %w", err)
		}
		resp, err = c.HTTPClient.Do(req)
		if err != nil {
			return "", nil, fmt.Errorf("failed to fetch data URL %s: %w", dataURL, err)
		}
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return "", nil, fmt.Errorf("HTTP error %d accessing data URL %s", resp.StatusCode, dataURL)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read response body: %w", err)
	}

	matches := zipRegex.FindAllStringSubmatch(string(body), -1)
	filesMap := make(map[string]bool)
	for _, m := range matches {
		if len(m) > 1 {
			filesMap[m[1]] = true
		}
	}

	var zipFiles []string
	for f := range filesMap {
		zipFiles = append(zipFiles, f)
	}
	sort.Strings(zipFiles)

	return dataURL, zipFiles, nil
}

// BuildFileURL resolves full HTTP URL for a specific file name
func (c *ReceitaCrawler) BuildFileURL(dataURL, filename string) (string, error) {
	base, err := url.Parse(dataURL)
	if err != nil {
		return "", err
	}
	ref, err := url.Parse(filename)
	if err != nil {
		return "", err
	}
	return base.ResolveReference(ref).String(), nil
}
