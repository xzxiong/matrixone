// Copyright 2022 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package fileservice

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	pathpkg "path"
	"sort"
	"strings"
	"time"

	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/util/trace"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// S3FS is a FileService implementation backed by S3
type S3FS struct {
	name      string
	client    *s3.Client
	bucket    string
	keyPrefix string

	memCache *MemCache
}

// key mapping scheme:
// <KeyPrefix>/<file path> -> file content

var _ FileService = new(S3FS)

func NewS3FS(
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int64,
) (*S3FS, error) {

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	return newS3FS(
		sharedConfigProfile,
		name,
		endpoint,
		bucket,
		keyPrefix,
		memCacheCapacity,
		nil,
		[]func(*s3.Options){
			s3.WithEndpointResolver(
				s3.EndpointResolverFromURL(endpoint),
			),
		},
	)
}

// NewS3FSOnMinio creates S3FS on minio server
// this is needed because the URL scheme of minio server does not compatible with AWS'
func NewS3FSOnMinio(
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int64,
) (*S3FS, error) {

	u, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if u.Scheme == "" {
		u.Scheme = "https"
	}
	endpoint = u.String()

	endpointResolver := s3.EndpointResolverFunc(
		func(
			region string,
			options s3.EndpointResolverOptions,
		) (
			ep aws.Endpoint,
			err error,
		) {
			_ = options
			ep.URL = endpoint
			ep.Source = aws.EndpointSourceCustom
			ep.HostnameImmutable = true
			ep.SigningRegion = region
			return
		},
	)

	return newS3FS(
		sharedConfigProfile,
		name,
		endpoint,
		bucket,
		keyPrefix,
		memCacheCapacity,
		nil,
		[]func(*s3.Options){
			s3.WithEndpointResolver(
				endpointResolver,
			),
		},
	)

}

func newS3FS(
	sharedConfigProfile string,
	name string,
	endpoint string,
	bucket string,
	keyPrefix string,
	memCacheCapacity int64,
	configOptions []func(*config.LoadOptions) error,
	s3Options []func(*s3.Options),
) (*S3FS, error) {

	if endpoint == "" {
		moerr.NewBadS3Config("empty endpoint")
	}
	if bucket == "" {
		moerr.NewBadS3Config("empty bucket")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*17)
	defer cancel()

	configOptions = append(configOptions,
		config.WithSharedConfigProfile(sharedConfigProfile),
		config.WithLogger(logutil.GetS3Logger()),
		config.WithClientLogMode(
			aws.LogSigning|
				aws.LogRetries|
				aws.LogRequest|
				aws.LogResponse|
				aws.LogDeprecatedUsage|
				aws.LogRequestEventMessage|
				aws.LogResponseEventMessage,
		),
	)
	cfg, err := config.LoadDefaultConfig(ctx,
		configOptions...,
	)
	if err != nil {
		return nil, err
	}

	client := s3.NewFromConfig(
		cfg,
		s3Options...,
	)

	// head bucket to validate config
	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: ptrTo(bucket),
	})
	if err != nil {
		return nil, moerr.NewInternalError("bad s3 config: %v", err)
	}

	fs := &S3FS{
		name:      name,
		client:    client,
		bucket:    bucket,
		keyPrefix: keyPrefix,
	}
	if memCacheCapacity > 0 {
		fs.memCache = NewMemCache(memCacheCapacity)
	}

	return fs, nil
}

func (s *S3FS) Name() string {
	return s.name
}

func (s *S3FS) List(ctx context.Context, dirPath string) (entries []DirEntry, err error) {
	ctx, span := trace.Start(ctx, "S3FS.List")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}

	path, err := ParsePathAtService(dirPath, s.name)
	if err != nil {
		return nil, err
	}
	prefix := s.pathToKey(path.File)
	if prefix != "" {
		prefix += "/"
	}
	var cont *string

	for {
		output, err := s.client.ListObjectsV2(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:            ptrTo(s.bucket),
				Delimiter:         ptrTo("/"),
				Prefix:            ptrTo(prefix),
				ContinuationToken: cont,
			},
		)
		if err != nil {
			return nil, err
		}

		for _, obj := range output.Contents {
			filePath := s.keyToPath(*obj.Key)
			filePath = strings.TrimRight(filePath, "/")
			_, name := pathpkg.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: false,
				Size:  obj.Size,
			})
		}

		for _, prefix := range output.CommonPrefixes {
			filePath := s.keyToPath(*prefix.Prefix)
			filePath = strings.TrimRight(filePath, "/")
			_, name := pathpkg.Split(filePath)
			entries = append(entries, DirEntry{
				Name:  name,
				IsDir: true,
			})
		}

		if output.ContinuationToken == nil ||
			*output.ContinuationToken == "" {
			break
		}
		cont = output.ContinuationToken
	}

	return
}

func (s *S3FS) Write(ctx context.Context, vector IOVector) error {
	ctx, span := trace.Start(ctx, "S3FS.Write")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}

	// check existence
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)
	output, err := s.client.HeadObject(
		ctx,
		&s3.HeadObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(key),
		},
	)
	if err != nil {
		var httpError *http.ResponseError
		if errors.As(err, &httpError) {
			if httpError.Response.StatusCode == 404 {
				// key not exists, ok
				err = nil
			}
		}
		if err != nil {
			return err
		}
	}
	if output != nil {
		// key existed
		return moerr.NewFileAlreadyExists(path.File)
	}

	return s.write(ctx, vector)
}

func (s *S3FS) write(ctx context.Context, vector IOVector) error {
	ctx, span := trace.Start(ctx, "S3FS.write")
	defer span.End()
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)

	// sort
	sort.Slice(vector.Entries, func(i, j int) bool {
		return vector.Entries[i].Offset < vector.Entries[j].Offset
	})

	// size
	var size int64
	if len(vector.Entries) > 0 {
		last := vector.Entries[len(vector.Entries)-1]
		size = int64(last.Offset + last.Size)
	}

	// put
	content, err := io.ReadAll(newIOEntriesReader(vector.Entries))
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(
		ctx,
		&s3.PutObjectInput{
			Bucket:        ptrTo(s.bucket),
			Key:           ptrTo(key),
			Body:          bytes.NewReader(content),
			ContentLength: size,
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3FS) Read(ctx context.Context, vector *IOVector) error {
	ctx, span := trace.Start(ctx, "S3FS.Read")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}

	if len(vector.Entries) == 0 {
		return moerr.NewEmptyVector()
	}

	if s.memCache == nil {
		// no cache
		return s.read(ctx, vector)
	}

	if err := s.memCache.Read(ctx, vector, s.read); err != nil {
		return err
	}

	return nil
}

func (s *S3FS) read(ctx context.Context, vector *IOVector) error {
	ctx, span := trace.Start(ctx, "S3FS.read")
	defer span.End()
	path, err := ParsePathAtService(vector.FilePath, s.name)
	if err != nil {
		return err
	}
	key := s.pathToKey(path.File)

	// calculate object read range
	min := int64(math.MaxInt)
	max := int64(0)
	readToEnd := false
	for _, entry := range vector.Entries {
		if entry.ignore {
			continue
		}
		if entry.Offset < min {
			min = entry.Offset
		}
		if entry.Size < 0 {
			entry.Size = 0
			readToEnd = true
		}
		if end := entry.Offset + entry.Size; end > max {
			max = end
		}
	}

	// a function to get an io.ReadCloser
	getReader := func(readToEnd bool, min int64, max int64) (io.ReadCloser, error) {
		ctx, span := trace.Start(ctx, "S3FS.read.getReader")
		defer span.End()
		if readToEnd {
			rang := fmt.Sprintf("bytes=%d-", min)
			output, err := s.client.GetObject(
				ctx,
				&s3.GetObjectInput{
					Bucket: ptrTo(s.bucket),
					Key:    ptrTo(key),
					Range:  ptrTo(rang),
				},
			)
			err = s.mapError(err, key)
			if err != nil {
				return nil, err
			}
			return output.Body, nil
		}

		rang := fmt.Sprintf("bytes=%d-%d", min, max)
		output, err := s.client.GetObject(
			ctx,
			&s3.GetObjectInput{
				Bucket: ptrTo(s.bucket),
				Key:    ptrTo(key),
				Range:  ptrTo(rang),
			},
		)
		err = s.mapError(err, key)
		if err != nil {
			return nil, err
		}
		return &readCloser{
			r:         io.LimitReader(output.Body, int64(max-min)),
			closeFunc: output.Body.Close,
		}, nil
	}

	// a function to get data lazily
	var contentBytes []byte
	var contentErr error
	var getContentDone bool
	getContent := func() (bs []byte, err error) {
		if getContentDone {
			return contentBytes, contentErr
		}
		defer func() {
			contentBytes = bs
			contentErr = err
			getContentDone = true
		}()

		reader, err := getReader(readToEnd, min, max)
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		bs, err = io.ReadAll(reader)
		err = s.mapError(err, key)
		if err != nil {
			return nil, err
		}

		return
	}

	for i, entry := range vector.Entries {
		if entry.ignore {
			continue
		}

		start := entry.Offset - min

		if entry.Size == 0 {
			return moerr.NewEmptyRange(path.File)
		} else if entry.Size > 0 {
			content, err := getContent()
			if err != nil {
				return err
			}
			if start >= int64(len(content)) {
				return moerr.NewEmptyRange(path.File)
			}
		}

		// a function to get entry data lazily
		getData := func() ([]byte, error) {
			if entry.Size < 0 {
				// read to end
				content, err := getContent()
				if err != nil {
					return nil, err
				}
				if start >= int64(len(content)) {
					return nil, moerr.NewEmptyRange(path.File)
				}
				return content[start:], nil
			}
			content, err := getContent()
			if err != nil {
				return nil, err
			}
			end := start + entry.Size
			if end > int64(len(content)) {
				return nil, moerr.NewUnexpectedEOF(path.File)
			}
			if start == end {
				return nil, moerr.NewEmptyRange(path.File)
			}
			return content[start:end], nil
		}

		setData := true

		if w := vector.Entries[i].WriterForRead; w != nil {
			setData = false
			if getContentDone {
				// data is ready
				data, err := getData()
				if err != nil {
					return err
				}
				_, err = w.Write(data)
				if err != nil {
					return err
				}

			} else {
				// get a reader and copy
				reader, err := getReader(entry.Size < 0, entry.Offset, entry.Offset+entry.Size)
				if err != nil {
					return err
				}
				defer reader.Close()
				_, err = io.Copy(w, reader)
				err = s.mapError(err, key)
				if err != nil {
					return err
				}
			}
		}

		if ptr := vector.Entries[i].ReadCloserForRead; ptr != nil {
			setData = false
			if getContentDone {
				// data is ready
				data, err := getData()
				if err != nil {
					return err
				}
				*ptr = io.NopCloser(bytes.NewReader(data))

			} else {
				// get a new reader
				reader, err := getReader(entry.Size < 0, entry.Offset, entry.Offset+entry.Size)
				if err != nil {
					return err
				}
				*ptr = &readCloser{
					r:         reader,
					closeFunc: reader.Close,
				}
			}
		}

		// set Data field
		if setData {
			data, err := getData()
			if err != nil {
				return err
			}
			if int64(len(entry.Data)) < entry.Size || entry.Size < 0 {
				entry.Data = data
			} else {
				copy(entry.Data, data)
			}
		}

		// set Object field
		if err := entry.setObjectFromData(); err != nil {
			return err
		}

		vector.Entries[i] = entry
	}

	return nil
}

func (s *S3FS) Delete(ctx context.Context, filePaths ...string) error {
	ctx, span := trace.Start(ctx, "S3FS.Delete")
	defer span.End()
	if ctx == nil {
		ctx = context.Background()
	}

	if len(filePaths) == 0 {
		return nil
	}
	if len(filePaths) == 1 {
		return s.deleteSingle(ctx, filePaths[0])
	}

	objs := make([]types.ObjectIdentifier, 0, 1000)
	for _, filePath := range filePaths {
		path, err := ParsePathAtService(filePath, s.name)
		if err != nil {
			return err
		}
		objs = append(objs, types.ObjectIdentifier{Key: ptrTo(s.pathToKey(path.File))})
		if len(objs) == 1000 {
			if err := s.deleteMultiObj(ctx, objs); err != nil {
				return err
			}
			objs = objs[:0]
		}
	}
	if err := s.deleteMultiObj(ctx, objs); err != nil {
		return err
	}
	return nil
}

func (s *S3FS) deleteMultiObj(ctx context.Context, objs []types.ObjectIdentifier) error {
	ctx, span := trace.Start(ctx, "S3FS.deleteMultiObj")
	defer span.End()
	output, err := s.client.DeleteObjects(ctx, &s3.DeleteObjectsInput{
		Bucket: ptrTo(s.bucket),
		Delete: &types.Delete{
			Objects: objs,
			// In quiet mode the response includes only keys where the delete action encountered an error.
			Quiet: true,
		},
	})
	// delete api failed
	if err != nil {
		return err
	}
	// delete api success, but with delete file failed.
	message := strings.Builder{}
	if len(output.Errors) > 0 {
		for _, Error := range output.Errors {
			if *Error.Code == (*types.NoSuchKey)(nil).ErrorCode() {
				continue
			}
			message.WriteString(fmt.Sprintf("%s: %s, %s;", *Error.Key, *Error.Code, *Error.Message))
		}
	}
	if message.Len() > 0 {
		return moerr.NewInternalError("S3 Delete failed: %s", message.String())
	}
	return nil
}

func (s *S3FS) deleteSingle(ctx context.Context, filePath string) error {
	ctx, span := trace.Start(ctx, "S3FS.deleteSingle")
	defer span.End()
	path, err := ParsePathAtService(filePath, s.name)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(
		ctx,
		&s3.DeleteObjectInput{
			Bucket: ptrTo(s.bucket),
			Key:    ptrTo(s.pathToKey(path.File)),
		},
	)
	if err != nil {
		return err
	}

	return nil
}

func (s *S3FS) pathToKey(filePath string) string {
	return pathpkg.Join(s.keyPrefix, filePath)
}

func (s *S3FS) keyToPath(key string) string {
	path := strings.TrimPrefix(key, s.keyPrefix)
	path = strings.TrimLeft(path, "/")
	return path
}

func (s *S3FS) mapError(err error, path string) error {
	if err == nil {
		return nil
	}
	var httpError *http.ResponseError
	if errors.As(err, &httpError) {
		if httpError.Response.StatusCode == 404 {
			return moerr.NewFileNotFound(path)
		}
	}
	return err
}

var _ ETLFileService = new(S3FS)

func (*S3FS) ETLCompatible() {}

var _ CachingFileService = new(S3FS)

func (s *S3FS) FlushCache() {
	if s.memCache != nil {
		s.memCache.Flush()
	}
}

func (s *S3FS) CacheStats() *CacheStats {
	if s.memCache != nil {
		return s.memCache.CacheStats()
	}
	return nil
}
