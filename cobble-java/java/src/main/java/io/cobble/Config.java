package io.cobble;

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Java-side config mapped to Rust {@code cobble::Config}.
 *
 * <p>Field names use camelCase in Java and serialize to snake_case JSON.
 */
public final class Config {
    private static final Gson GSON =
            new GsonBuilder()
                    .setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES)
                    .create();

    /** Storage volumes for metadata/data/cache/snapshot usage. */
    public List<VolumeDescriptor> volumes;

    /** Memtable capacity in bytes. */
    public Integer memtableCapacity;

    /** Number of memtable buffers kept in memory. */
    public Integer memtableBufferCount;

    /** Memtable implementation type. */
    public MemtableType memtableType;

    /** Number of value columns in schema. */
    public Integer numColumns;

    /** Total number of buckets in cluster (1..65536). */
    public Integer totalBuckets;

    /** Max L0 file count before compaction scheduling. */
    public Integer l0FileLimit;

    /** Optional write-stall threshold for immutables + L0 files. */
    public Integer writeStallLimit;

    /** Base size in bytes for level-1. */
    public Integer l1BaseBytes;

    /** Size multiplier between adjacent LSM levels. */
    public Integer levelSizeMultiplier;

    /** Maximum LSM level number. */
    public Integer maxLevel;

    /** Compaction policy selection. */
    public CompactionPolicyKind compactionPolicy;

    /** Whether compaction read-ahead is enabled. */
    public Boolean compactionReadAheadEnabled;

    /** Optional remote compaction worker endpoint (host:port). */
    public String compactionRemoteAddr;

    /** Remote compaction worker thread count. */
    public Integer compactionThreads;

    /** Remote compaction timeout in milliseconds. */
    public Long compactionRemoteTimeoutMs;

    /** Block cache memory size in bytes (0 means disabled). */
    public Integer blockCacheSize;

    /** Whether hybrid (memory + disk) block cache is enabled. */
    public Boolean blockCacheHybridEnabled;

    /** Optional hybrid block-cache disk capacity in bytes. */
    public Integer blockCacheHybridDiskSize;

    /** Read-proxy/reader-specific overrides. */
    public ReaderConfigEntry reader;

    /** Target SST base file size in bytes. */
    public Integer baseFileSize;

    /** Whether SST bloom filter is enabled. */
    public Boolean sstBloomFilterEnabled;

    /** Bloom filter bits-per-key when bloom is enabled. */
    public Integer sstBloomBitsPerKey;

    /** Whether SST partitioned index/filter blocks are enabled. */
    public Boolean sstPartitionedIndex;

    /** Data-file format for flush/compaction output. */
    public DataFileType dataFileType;

    /** Target parquet row-group size in bytes. */
    public Integer parquetRowGroupSizeBytes;

    /** Compression algorithm by LSM level index. */
    public List<SstCompressionAlgorithm> sstCompressionByLevel;

    /** Whether TTL is enabled. */
    public Boolean ttlEnabled;

    /** Default TTL in seconds, null means no default expiration. */
    public Integer defaultTtlSeconds;

    /** Separation threshold in bytes for value-log offload. */
    public Integer valueSeparationThreshold;

    /** Time provider implementation used by TTL. */
    public TimeProviderKind timeProvider;

    /** Optional local log file path. */
    public String logPath;

    /** Whether logs should also be written to console. */
    public Boolean logConsole;

    /** Log level filter (trace/debug/info/warn/error/off). */
    public String logLevel;

    /** Whether to take snapshot automatically on each successful flush. */
    public Boolean snapshotOnFlush;

    /** Ratio threshold for incremental active-memtable snapshot optimization. */
    public Double activeMemtableIncrementalSnapshotRatio;

    /** Optional level whose overflow triggers tree split. */
    public Integer lsmSplitTriggerLevel;

    /** Watermark for stopping writes on pressured primary volumes. */
    public Double primaryVolumeWriteStopWatermark;

    /** Watermark for triggering background primary-volume offload. */
    public Double primaryVolumeOffloadTriggerWatermark;

    /** Candidate-selection policy for primary-volume offload. */
    public PrimaryVolumeOffloadPolicyKind primaryVolumeOffloadPolicy;

    /** Keep at most this many recent snapshots (null disables auto-expire). */
    public Integer snapshotRetention;

    public Config addVolume(String baseDir) {
        if (baseDir == null || baseDir.trim().isEmpty()) {
            throw new IllegalArgumentException("baseDir must not be empty");
        }
        if (volumes == null) {
            volumes = new ArrayList<VolumeDescriptor>();
        }
        volumes.add(VolumeDescriptor.singleVolume(baseDir));
        return this;
    }

    public Config addVolume(VolumeDescriptor volume) {
        if (volume == null) {
            throw new IllegalArgumentException("volume must not be null");
        }
        if (volumes == null) {
            volumes = new ArrayList<VolumeDescriptor>();
        }
        volumes.add(volume);
        return this;
    }

    public Config numColumns(int value) {
        numColumns = value;
        return this;
    }

    public Config totalBuckets(int value) {
        totalBuckets = value;
        return this;
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    public static final class ReaderConfigEntry {
        /** Number of partitions pinned in memory. */
        public Integer pinPartitionInMemoryCount;

        /** Reader block-cache size in bytes. */
        public Integer blockCacheSize;

        /** Reload tolerance window in seconds. */
        public Long reloadToleranceSeconds;
    }

    public static final class VolumeDescriptor {
        /** Volume base directory URL/path. */
        public String baseDir;

        /** Optional storage access id. */
        public String accessId;

        /** Optional storage secret key. */
        public String secretKey;

        /** Optional volume size limit in bytes. */
        public Long sizeLimit;

        /** Usage kinds supported by this volume. */
        public List<VolumeUsageKind> kinds;

        /** Optional backend-specific initialization options. */
        public Map<String, String> customOptions;

        public static VolumeDescriptor singleVolume(String baseDir) {
            VolumeDescriptor descriptor = new VolumeDescriptor();
            descriptor.baseDir = baseDir;
            descriptor.kinds =
                    Arrays.asList(VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH, VolumeUsageKind.META);
            return descriptor;
        }
    }

    public enum MemtableType {
        @SerializedName("hash")
        HASH,
        @SerializedName("skiplist")
        SKIPLIST,
        @SerializedName("vec")
        VEC
    }

    public enum CompactionPolicyKind {
        @SerializedName("round_robin")
        ROUND_ROBIN,
        @SerializedName("min_overlap")
        MIN_OVERLAP
    }

    public enum PrimaryVolumeOffloadPolicyKind {
        @SerializedName("largest_file")
        LARGEST_FILE,
        @SerializedName("priority")
        PRIORITY
    }

    public enum DataFileType {
        @SerializedName("sst")
        SST,
        @SerializedName("parquet")
        PARQUET
    }

    public enum SstCompressionAlgorithm {
        @SerializedName("none")
        NONE,
        @SerializedName("lz4")
        LZ4
    }

    public enum TimeProviderKind {
        @SerializedName("system")
        SYSTEM,
        @SerializedName("manual")
        MANUAL
    }

    public enum VolumeUsageKind {
        @SerializedName("meta")
        META,
        @SerializedName("primary_data_priority_high")
        PRIMARY_DATA_PRIORITY_HIGH,
        @SerializedName("primary_data_priority_medium")
        PRIMARY_DATA_PRIORITY_MEDIUM,
        @SerializedName("primary_data_priority_low")
        PRIMARY_DATA_PRIORITY_LOW,
        @SerializedName("snapshot")
        SNAPSHOT,
        @SerializedName("cache")
        CACHE,
        @SerializedName("readonly")
        READONLY
    }
}
