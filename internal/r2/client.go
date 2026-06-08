package r2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
)

// ErrNotFound is returned by GetObject / DeleteObject when the key is absent.
var ErrNotFound = errors.New("r2: no such key")

// Client wraps the S3-compatible Cloudflare R2 API.
type Client struct {
	svc    *s3.Client
	bucket string
}

// Config holds connection parameters for a Cloudflare R2 bucket.
type Config struct {
	// Endpoint is the account-level R2 endpoint URL:
	//   https://<account_id>.r2.cloudflarestorage.com
	Endpoint        string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
}

// New creates a Client for the given R2 bucket.
func New(cfg Config) (*Client, error) {
	ac, err := awsconfig.LoadDefaultConfig(context.Background(),
		awsconfig.WithRegion("auto"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, ""),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("r2: load config: %w", err)
	}

	// Instrument all AWS SDK calls with OpenTelemetry spans.
	otelaws.AppendMiddlewares(&ac.APIOptions)

	svc := s3.NewFromConfig(ac, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})

	return &Client{svc: svc, bucket: cfg.Bucket}, nil
}

// PutObject writes data to the given object key (overwrites if exists).
func (c *Client) PutObject(ctx context.Context, key string, data []byte) error {
	_, err := c.svc.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(c.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(data),
		ContentLength: aws.Int64(int64(len(data))),
	})
	if err != nil {
		return fmt.Errorf("r2 put %q: %w", key, err)
	}
	return nil
}

// GetObject downloads the bytes for the given object key.
// Returns ErrNotFound when the key does not exist.
func (c *Client) GetObject(ctx context.Context, key string) ([]byte, error) {
	out, err := c.svc.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("r2 get %q: %w", key, err)
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			// ignore
		}
	}(out.Body)

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("r2 get %q read: %w", key, err)
	}
	return data, nil
}

// ── Storage Statistics ────────────────────────────────────────────────────────

// InstrumentStorage holds aggregated file and byte counts for one instrument.
type InstrumentStorage struct {
	Name        string  `json:"name"`
	TickFiles   int64   `json:"tick_files"`
	TickBytes   int64   `json:"tick_bytes"`
	TickMB      float64 `json:"tick_mb"`
	CandleFiles int64   `json:"candle_files"`
	CandleBytes int64   `json:"candle_bytes"`
	CandleMB    float64 `json:"candle_mb"`
	TotalFiles  int64   `json:"total_files"`
	TotalBytes  int64   `json:"total_bytes"`
	TotalMB     float64 `json:"total_mb"`
}

// StorageStatsResult is the full bucket storage snapshot.
type StorageStatsResult struct {
	Bucket      string              `json:"bucket"`
	TotalFiles  int64               `json:"total_files"`
	TotalBytes  int64               `json:"total_bytes"`
	TotalMB     float64             `json:"total_mb"`
	TickFiles   int64               `json:"tick_files"`
	CandleFiles int64               `json:"candle_files"`
	Instruments []InstrumentStorage `json:"instruments"`
	ComputedAt  time.Time           `json:"computed_at"`
}

// storageCache holds the last computed result and its expiry.
var (
	storageCacheMu  sync.Mutex
	storageCached   *StorageStatsResult
	storageCacheExp time.Time
	storageCacheTTL = 5 * time.Minute
)

// StorageStats lists all objects under the ingestion prefix and aggregates
// file counts and byte totals per instrument and job type.
// Results are cached for 5 minutes to avoid hammering the object store.
func (c *Client) StorageStats(ctx context.Context) (StorageStatsResult, error) {
	storageCacheMu.Lock()
	if storageCached != nil && time.Now().Before(storageCacheExp) {
		cached := *storageCached
		storageCacheMu.Unlock()
		return cached, nil
	}
	storageCacheMu.Unlock()

	// Paginate through all objects under the ingestion/ prefix.
	paginator := s3.NewListObjectsV2Paginator(c.svc, &s3.ListObjectsV2Input{
		Bucket: aws.String(c.bucket),
		Prefix: aws.String("ingestion/dukascopy/"),
	})

	// instrument name → running totals
	type counter struct {
		tickFiles, tickBytes, candleFiles, candleBytes int64
	}
	byInstrument := map[string]*counter{}

	var totalFiles, totalBytes int64

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return StorageStatsResult{}, fmt.Errorf("r2 list storage: %w", err)
		}
		for _, obj := range page.Contents {
			key := aws.ToString(obj.Key)
			size := aws.ToInt64(obj.Size)
			totalFiles++
			totalBytes += size

			// Key pattern: ingestion/dukascopy/{ticks|candles}/{instrument}/...
			parts := strings.SplitN(key, "/", 5) // [ingestion, dukascopy, type, instrument, rest]
			if len(parts) < 4 {
				continue
			}
			jobType, inst := parts[2], parts[3]
			if _, ok := byInstrument[inst]; !ok {
				byInstrument[inst] = &counter{}
			}
			c2 := byInstrument[inst]
			switch jobType {
			case "ticks":
				c2.tickFiles++
				c2.tickBytes += size
			case "candles":
				c2.candleFiles++
				c2.candleBytes += size
			}
		}
	}

	round2 := func(b int64) float64 { return math.Round(float64(b)/1024/1024*100) / 100 }

	instruments := make([]InstrumentStorage, 0, len(byInstrument))
	var totalTickFiles, totalCandleFiles int64
	for name, c2 := range byInstrument {
		totalTickFiles += c2.tickFiles
		totalCandleFiles += c2.candleFiles
		instruments = append(instruments, InstrumentStorage{
			Name:        name,
			TickFiles:   c2.tickFiles,
			TickBytes:   c2.tickBytes,
			TickMB:      round2(c2.tickBytes),
			CandleFiles: c2.candleFiles,
			CandleBytes: c2.candleBytes,
			CandleMB:    round2(c2.candleBytes),
			TotalFiles:  c2.tickFiles + c2.candleFiles,
			TotalBytes:  c2.tickBytes + c2.candleBytes,
			TotalMB:     round2(c2.tickBytes + c2.candleBytes),
		})
	}

	result := StorageStatsResult{
		Bucket:      c.bucket,
		TotalFiles:  totalFiles,
		TotalBytes:  totalBytes,
		TotalMB:     round2(totalBytes),
		TickFiles:   totalTickFiles,
		CandleFiles: totalCandleFiles,
		Instruments: instruments,
		ComputedAt:  time.Now().UTC(),
	}

	storageCacheMu.Lock()
	storageCached = &result
	storageCacheExp = time.Now().Add(storageCacheTTL)
	storageCacheMu.Unlock()

	return result, nil
}

// DeleteObject removes the key from R2.
// Returns ErrNotFound when the key does not exist (safe to ignore in Pruning sweep).
func (c *Client) DeleteObject(ctx context.Context, key string) error {
	_, err := c.svc.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(c.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var nsk *s3types.NoSuchKey
		if errors.As(err, &nsk) {
			return ErrNotFound
		}
		return fmt.Errorf("r2 delete %q: %w", key, err)
	}
	return nil
}
