// Package dukascopy provides an HTTP client for fetching tick data from
// Dukascopy's public datafeed and a parser for the BI5 binary format.
package dukascopy

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
)

// ErrNotFound is returned when Dukascopy responds with HTTP 404 (no data for
// that hour) or when the server returns an empty body.
var ErrNotFound = errors.New("dukascopy: 404 not found")

const baseURL = "https://datafeed.dukascopy.com/datafeed/"

// Client fetches BI5 tick files from Dukascopy's public data feed.
type Client struct {
	http *http.Client
}

// NewClient returns a Dukascopy Client with a 30-second per-request timeout.
// Outbound HTTP calls are wrapped with an OpenTelemetry transport so each
// request produces a child span.
func NewClient() *Client {
	return &Client{
		http: &http.Client{
			Timeout:   30 * time.Second,
			Transport: otelhttp.NewTransport(http.DefaultTransport),
		},
	}
}

// FetchBI5 downloads the LZMA-compressed BI5 file for the given instrument
// and UTC hour.  Returns ErrNotFound when Dukascopy has no data for that slot.
//
// URL pattern (Dukascopy uses 0-indexed months — January = 0, December = 11):
//
//	https://datafeed.dukascopy.com/datafeed/{SYM}/{YYYY}/{MM_0IDX}/{DD}/{HH}h_ticks.bi5
func (c *Client) FetchBI5(ctx context.Context, instrument string, ts time.Time) ([]byte, error) {
	sym := strings.ToUpper(instrument)
	t := ts.UTC()

	url := fmt.Sprintf("%s%s/%04d/%02d/%02d/%02dh_ticks.bi5",
		baseURL, sym,
		t.Year(),
		int(t.Month())-1, // 0-indexed
		t.Day(),
		t.Hour(),
	)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("dukascopy: build request: %w", err)
	}
	req.Header.Set("User-Agent", "geonera-ingestion/1.0")

	resp, err := c.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("dukascopy: %s: %w", url, err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case http.StatusOK:
		data, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, fmt.Errorf("dukascopy: read body: %w", err)
		}
		if len(data) == 0 {
			// Empty body = no ticks for this hour.
			return nil, ErrNotFound
		}
		return data, nil
	case http.StatusNotFound:
		return nil, ErrNotFound
	default:
		return nil, fmt.Errorf("dukascopy: status %d for %s", resp.StatusCode, url)
	}
}
