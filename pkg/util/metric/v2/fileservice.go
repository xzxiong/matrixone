// Copyright 2023 Matrix Origin
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

package v2

import (
	"github.com/prometheus/client_golang/prometheus"
)

var (
	S3ConnectCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_connect_total",
			Help:      "Total number of s3 connect count.",
		})

	S3DNSResolveCounter = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_dns_resolve_total",
			Help:      "Total number of s3 dns resolve count.",
		})

	fsReadCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "read_total",
			Help:      "Total number of read count.",
		}, []string{"type"})
	FSReadS3Counter        = fsReadCounter.WithLabelValues("s3")
	FSReadLocalCounter     = fsReadCounter.WithLabelValues("local")
	FSReadHitMemCounter    = fsReadCounter.WithLabelValues("hit-mem")
	FSReadHitDiskCounter   = fsReadCounter.WithLabelValues("hit-disk")
	FSReadHitRemoteCounter = fsReadCounter.WithLabelValues("hit-remote")
	FSReadHitMetaCounter   = fsReadCounter.WithLabelValues("hit-meta")
	FSReadReadMemCounter   = fsReadCounter.WithLabelValues("read-mem")
	FSReadReadDiskCounter  = fsReadCounter.WithLabelValues("read-disk")
	FSReadReadMetaCounter  = fsReadCounter.WithLabelValues("read-meta")
)

var (
	s3IOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_io_bytes",
			Help:      "Bucketed histogram of s3 io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})
	S3WriteIOBytesHistogram = s3IOBytesHistogram.WithLabelValues("write")
	S3ReadIOBytesHistogram  = s3IOBytesHistogram.WithLabelValues("read")

	s3ConnDurationHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "s3_conn_duration_seconds",
			Help:      "Bucketed histogram of s3 get conn duration.",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	S3GetConnDurationHistogram      = s3ConnDurationHistogram.WithLabelValues("get-conn")
	S3DNSResolveDurationHistogram   = s3ConnDurationHistogram.WithLabelValues("dns-resolve")
	S3ConnectDurationHistogram      = s3ConnDurationHistogram.WithLabelValues("connect")
	S3TLSHandshakeDurationHistogram = s3ConnDurationHistogram.WithLabelValues("tls-handshake")

	localIOBytesHistogram = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "local_io_bytes",
			Help:      "Bucketed histogram of local io bytes.",
			Buckets:   prometheus.ExponentialBuckets(1, 2.0, 10),
		}, []string{"type"})
	LocalWriteIOBytesHistogram = localIOBytesHistogram.WithLabelValues("write")
	LocalReadIOBytesHistogram  = localIOBytesHistogram.WithLabelValues("read")
)

var (
	ioMergerCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "io_merger_counter",
			Help:      "io merger counter",
		},
		[]string{"type"},
	)
	IOMergerCounterInitiate = ioMergerCounter.WithLabelValues("initiate")
	IOMergerCounterWait     = ioMergerCounter.WithLabelValues("wait")

	ioMergerDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "io_merger_duration_seconds",
			Help:      "io merger duration seconds",
			Buckets:   getDurationBuckets(),
		}, []string{"type"})
	IOMergerDurationInitiate = ioMergerDuration.WithLabelValues("initiate")
	IOMergerDurationWait     = ioMergerDuration.WithLabelValues("wait")
)

var (
	fsReadWriteDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "read_write_duration",
			Help:      "read write duration",
			Buckets:   getDurationBuckets(),
		},
		[]string{"type"},
	)
	FSReadDurationReadVectorCache   = fsReadWriteDuration.WithLabelValues("read-vector-cache")
	FSReadDurationUpdateVectorCache = fsReadWriteDuration.WithLabelValues("update-vector-cache")
	FSReadDurationReadMemoryCache   = fsReadWriteDuration.WithLabelValues("read-memory-cache")
	FSReadDurationUpdateMemoryCache = fsReadWriteDuration.WithLabelValues("update-memory-cache")
	FSReadDurationReadDiskCache     = fsReadWriteDuration.WithLabelValues("read-disk-cache")
	FSReadDurationUpdateDiskCache   = fsReadWriteDuration.WithLabelValues("update-disk-cache")
	FSReadDurationReadRemoteCache   = fsReadWriteDuration.WithLabelValues("read-remote-cache")
	FSReadDurationGetReader         = fsReadWriteDuration.WithLabelValues("get-reader")
	FSReadDurationGetContent        = fsReadWriteDuration.WithLabelValues("get-content")
	FSReadDurationGetEntryData      = fsReadWriteDuration.WithLabelValues("get-entry-data")
	FSReadDurationWriteToWriter     = fsReadWriteDuration.WithLabelValues("write-to-writer")
	FSReadDurationSetCachedData     = fsReadWriteDuration.WithLabelValues("set-cached-data")
	FSReadDurationDiskCacheSetFile  = fsReadWriteDuration.WithLabelValues("disk-cache-set-file")
	FSReadDurationList              = fsReadWriteDuration.WithLabelValues("list")
	FSReadDurationStat              = fsReadWriteDuration.WithLabelValues("stat")
	FSReadDurationIOReadAll         = fsReadWriteDuration.WithLabelValues("io-read-all")

	FSWriteDurationWrite = fsReadWriteDuration.WithLabelValues("write")
)

var (
	FSObjectStorageOperations = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "object_storage_operations",
			Help:      "object storage operations",
		},
		[]string{
			"name",
			"op",
		},
	)
)

var (
	fsCacheBytes = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: "mo",
			Subsystem: "fs",
			Name:      "cache_bytes",
			Help:      "Total bytes of fs cache.",
		}, []string{"name", "type"})
	FSCacheInuseMemBytes  = fsCacheBytes.WithLabelValues("inuse-mem")
	FSCacheInuseDiskBytes = fsCacheBytes.WithLabelValues("inuse-disk")
	FSCacheInuseMetaBytes = fsCacheBytes.WithLabelValues("inuse-meta")
	FSCacheCapMemBytes    = fsCacheBytes.WithLabelValues("cap-mem")
	FSCacheCapDiskBytes   = fsCacheBytes.WithLabelValues("cap-disk")
	FSCacheCapMetaBytes   = fsCacheBytes.WithLabelValues("cap-meta")
)

// GetFsCacheBytesGauge return inuse, cap Gauge metric
// {typ} should be [mem, disk, meta]
func GetFsCacheBytesGauge(name, typ string) (inuse prometheus.Gauge, capacity prometheus.Gauge) {
	return fsCacheBytes.WithLabelValues(name, "inuse-"+typ),
		fsCacheBytes.WithLabelValues(name, "cap-"+typ)
}
