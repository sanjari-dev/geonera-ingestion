package r2

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"go.opentelemetry.io/contrib/instrumentation/github.com/aws/aws-sdk-go-v2/otelaws"
)

// ErrNotFound is returned by GetObject / DeleteObject when the key is absent.
var ErrNotFound = errors.New("R2: no such key")

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
	defer out.Body.Close()

	data, err := io.ReadAll(out.Body)
	if err != nil {
		return nil, fmt.Errorf("r2 get %q read: %w", key, err)
	}
	return data, nil
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
