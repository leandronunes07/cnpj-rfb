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
	monthRegex = regexp.MustCompile(`href="(\d{4}-\d{2})/"`)
	zipRegex   = regexp.MustCompile(`href="([^"]+\.zip)"`)
)

type ReceitaCrawler struct {
	BaseURL    string
	HTTPClient *http.Client
}

func NewReceitaCrawler(baseURL string) *ReceitaCrawler {
	return &ReceitaCrawler{
		BaseURL: baseURL,
		HTTPClient: &http.Client{
			Timeout: 60 * time.Second,
		},
	}
}

// GetLatestMonth fetches the base directory HTML and returns the latest YYYY-MM folder
func (c *ReceitaCrawler) GetLatestMonth() (string, error) {
	resp, err := c.HTTPClient.Get(c.BaseURL)
	if err != nil {
		return "", fmt.Errorf("failed to fetch base URL %s: %w", c.BaseURL, err)
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

// ListZipFiles fetches the HTML for a month directory and returns list of .zip filenames
func (c *ReceitaCrawler) ListZipFiles(dataMonth string) (string, []string, error) {
	dataURL := fmt.Sprintf("%s%s/", strings.TrimSuffix(c.BaseURL, "/"), dataMonth)

	resp, err := c.HTTPClient.Get(dataURL)
	if err != nil {
		return "", nil, fmt.Errorf("failed to fetch data URL %s: %w", dataURL, err)
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
