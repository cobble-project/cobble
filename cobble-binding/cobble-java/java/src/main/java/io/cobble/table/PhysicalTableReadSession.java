package io.cobble.table;

import io.cobble.Config;
import io.cobble.DirectScanCursor;
import io.cobble.ReadOptions;
import io.cobble.Reader;
import io.cobble.ScanOptions;

import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;

/**
 * Shared fixed-snapshot session for format bindings over Cobble's raw Reader.
 *
 * <p>Physical resource ownership, bucket traversal, direct-cursor lifetime, and durable logical row
 * positions live here. A binding supplies only schema, key encoding, physical projection, and
 * transient row decoding.
 */
final class PhysicalTableReadSession implements TableReadSession<List<Value>, List<Value>> {
    private final Reader physical;
    private final String columnFamily;
    private final TableFormatBinding binding;
    private final int totalBuckets;
    private final int[] physicalColumns;
    private final ReadOptions readOptions;
    private final ScanOptions scanOptions;
    private final List<io.cobble.ShardSnapshot.Range> snapshotRanges;
    private final boolean[] ownedBuckets;
    private final List<TableReadCursor<List<Value>>> cursors = new ArrayList<>();
    private boolean closed;

    PhysicalTableReadSession(
            Config config, TableReadSnapshot snapshot, TableFormatBinding binding) {
        Objects.requireNonNull(config, "config");
        Objects.requireNonNull(snapshot, "snapshot");
        this.binding = Objects.requireNonNull(binding, "binding");
        this.totalBuckets = snapshot.globalSnapshot().totalBuckets;
        this.columnFamily = snapshot.columnFamily();
        this.snapshotRanges = snapshotRanges(snapshot);
        this.ownedBuckets = ownedBuckets(totalBuckets, snapshotRanges);
        int[] requestedColumns = binding.physicalColumns();
        this.physicalColumns =
                requestedColumns.length == 0 ? new int[] {0} : requestedColumns.clone();
        for (int column : physicalColumns) {
            if (column < 0)
                throw new IllegalArgumentException("physical column index must be >= 0");
        }
        Reader opened = Reader.open(physicalConfig(config, snapshot), snapshot.globalSnapshot());
        ReadOptions configuredRead = null;
        ScanOptions configuredScan = null;
        try {
            configuredRead = ReadOptions.forColumnsInFamily(columnFamily, physicalColumns);
            configuredScan = new ScanOptions().columnFamily(columnFamily).columns(physicalColumns);
            this.physical = opened;
            this.readOptions = configuredRead;
            this.scanOptions = configuredScan;
        } catch (RuntimeException error) {
            if (configuredRead != null) configuredRead.close();
            if (configuredScan != null) configuredScan.close();
            opened.close();
            throw error;
        }
    }

    @Override
    public TableReadSchema schema() {
        ensureOpen();
        return binding.schema();
    }

    @Override
    public TableReadCapabilities capabilities() {
        ensureOpen();
        return binding.capabilities();
    }

    @Override
    public List<DataField> keyFields() {
        ensureOpen();
        return binding.keyFields();
    }

    @Override
    public TableReadCursor<List<Value>> scan(TableReadRange range, TableReadPosition position)
            throws Exception {
        return scan(range, position, snapshotRanges);
    }

    TableReadCursor<List<Value>> scan(
            TableReadRange range,
            TableReadPosition position,
            List<io.cobble.ShardSnapshot.Range> ownedRanges)
            throws Exception {
        ensureOpen();
        Objects.requireNonNull(range, "range");
        List<Integer> buckets = selectedBuckets(totalBuckets, range, ownedRanges);
        TableReadCursor<List<Value>> cursor =
                TableDirectScanCursor.buckets(
                        buckets,
                        position,
                        null,
                        this::openCursor,
                        (entry, key) -> binding.decodeDirect(entry, entry.getBucket(), key));
        return retain(cursor);
    }

    static List<Integer> selectedBuckets(
            int totalBuckets,
            TableReadRange range,
            List<io.cobble.ShardSnapshot.Range> ownedRanges) {
        int first = Math.max(0, range.firstBucket());
        int last = Math.min(totalBuckets - 1, range.lastBucket());
        boolean[] selected = new boolean[totalBuckets];
        if (ownedRanges == null) {
            for (int bucket = first; bucket <= last; bucket++) selected[bucket] = true;
        } else {
            for (io.cobble.ShardSnapshot.Range owned : ownedRanges) {
                int start = Math.max(first, owned.start);
                int end = Math.min(last, owned.end);
                for (int bucket = start; bucket <= end; bucket++) selected[bucket] = true;
            }
        }
        List<Integer> buckets = new ArrayList<Integer>(Math.max(0, last - first + 1));
        for (int bucket = first; bucket <= last; bucket++) {
            if (selected[bucket]) buckets.add(Integer.valueOf(bucket));
        }
        return buckets;
    }

    private static List<io.cobble.ShardSnapshot.Range> snapshotRanges(TableReadSnapshot snapshot) {
        List<io.cobble.ShardSnapshot.Range> result = new ArrayList<io.cobble.ShardSnapshot.Range>();
        for (TableReadSnapshot.ShardDescriptor shard : snapshot.shards()) {
            result.addAll(shard.snapshot().ranges);
        }
        return Collections.unmodifiableList(result);
    }

    private static boolean[] ownedBuckets(
            int totalBuckets, List<io.cobble.ShardSnapshot.Range> ranges) {
        boolean[] result = new boolean[totalBuckets];
        for (io.cobble.ShardSnapshot.Range range : ranges) {
            int start = Math.max(0, range.start);
            int end = Math.min(totalBuckets - 1, range.end);
            for (int bucket = start; bucket <= end; bucket++) result[bucket] = true;
        }
        return result;
    }

    @Override
    public Collection<TableReadEntry<List<Value>>> lookup(List<Value> key) throws Exception {
        ensureOpen();
        if (!binding.capabilities().exactLookup()) {
            throw new UnsupportedOperationException("exact lookup is not supported by this format");
        }
        TablePhysicalKey encoded = binding.encodeKey(Objects.requireNonNull(key, "key"));
        if (encoded.bucket() < 0
                || encoded.bucket() >= ownedBuckets.length
                || !ownedBuckets[encoded.bucket()]) return Collections.emptyList();
        byte[] keyBytes = encoded.bytes();
        byte[][] columns;
        try {
            columns = physical.getWithOptions(encoded.bucket(), keyBytes, readOptions);
        } catch (RuntimeException error) {
            if (binding.isMissingColumnFamily(error)) return Collections.emptyList();
            throw error;
        }
        if (columns == null) return Collections.emptyList();
        List<List<Value>> rows = binding.decode(encoded.bucket(), keyBytes, columns);
        if (rows.size() > 1) {
            throw new IllegalStateException("exact lookup decoded more than one logical row");
        }
        if (rows.isEmpty()) return Collections.emptyList();
        return Collections.singletonList(
                new TableReadEntry<List<Value>>(
                        new TableReadPosition(encoded.bucket(), keyBytes, 1),
                        rows.get(0),
                        physicalBytes(keyBytes, columns),
                        true));
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        RuntimeException failure = null;
        for (TableReadCursor<List<Value>> cursor : new ArrayList<>(cursors)) {
            try {
                cursor.close();
            } catch (RuntimeException error) {
                failure = addFailure(failure, error);
            }
        }
        cursors.clear();
        try {
            readOptions.close();
        } catch (RuntimeException error) {
            failure = addFailure(failure, error);
        }
        try {
            scanOptions.close();
        } catch (RuntimeException error) {
            failure = addFailure(failure, error);
        }
        try {
            physical.close();
        } catch (RuntimeException error) {
            failure = addFailure(failure, error);
        }
        if (failure != null) throw failure;
    }

    private static RuntimeException addFailure(RuntimeException failure, RuntimeException next) {
        if (failure == null) {
            return next;
        }
        failure.addSuppressed(next);
        return failure;
    }

    private DirectScanCursor openCursor(int bucket, byte[] start, byte[] end) {
        try {
            return physical.scanDirectWithOptions(bucket, start, end, scanOptions);
        } catch (RuntimeException error) {
            if (binding.isMissingColumnFamily(error)) return null;
            throw error;
        }
    }

    private TableReadCursor<List<Value>> retain(TableReadCursor<List<Value>> delegate) {
        TableReadCursor<List<Value>> tracked = new TrackedCursor(delegate);
        cursors.add(tracked);
        return tracked;
    }

    private final class TrackedCursor implements TableReadCursor<List<Value>> {
        private final TableReadCursor<List<Value>> delegate;
        private boolean cursorClosed;

        private TrackedCursor(TableReadCursor<List<Value>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public TableReadEntry<List<Value>> next() throws Exception {
            if (cursorClosed) {
                throw new IllegalStateException("physical table scan cursor is closed");
            }
            try {
                return delegate.next();
            } catch (Exception error) {
                close();
                throw error;
            }
        }

        @Override
        public void close() {
            if (cursorClosed) return;
            cursorClosed = true;
            try {
                delegate.close();
            } finally {
                cursors.remove(this);
            }
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("physical table read session is closed");
    }

    private static long physicalBytes(byte[] key, byte[][] columns) {
        long result = key.length;
        for (byte[] column : columns) {
            if (column != null) result += column.length;
        }
        return result;
    }

    static Config physicalConfig(Config source, TableReadSnapshot snapshot) {
        Config result = source.copy();
        LinkedHashSet<String> roots = new LinkedHashSet<String>();
        for (TableReadSnapshot.ShardDescriptor shard : snapshot.shards()) {
            String manifest = shard.snapshot().manifestPath;
            if (manifest == null || manifest.trim().isEmpty()) {
                throw new IllegalArgumentException(
                        "fixed shard snapshot is missing its manifest path");
            }
            // Callers may pass a SNAPSHOT-* file through Config.addVolume verbatim. That is an
            // input path, not a storage root; retaining it would make native resolution try to
            // open <manifest-file>/<db-id>/... before the derived root is considered.
            removeExactManifestInputVolumes(result, manifest);
            if (!hasContainingVolume(result, manifest)) {
                String root = manifestRoot(manifest, shard.snapshot().dbId);
                if (roots.add(root)) result.addVolume(derivedVolume(source, root, manifest));
            }
        }
        return result;
    }

    private static void removeExactManifestInputVolumes(Config config, String manifest) {
        if (!isSnapshotManifest(manifest) || config.volumes == null) return;
        for (Iterator<Config.VolumeDescriptor> it = config.volumes.iterator(); it.hasNext(); ) {
            Config.VolumeDescriptor volume = it.next();
            if (volume != null && samePath(volume.baseDir, manifest)) it.remove();
        }
    }

    private static Config.VolumeDescriptor derivedVolume(
            Config source, String root, String manifest) {
        Config template = source.copy();
        Config.VolumeDescriptor volume = matchingVolume(template, manifest);
        if (volume == null) volume = new Config.VolumeDescriptor();
        volume.baseDir = root;
        volume.kinds =
                java.util.Arrays.asList(
                        Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH,
                        Config.VolumeUsageKind.META,
                        Config.VolumeUsageKind.SNAPSHOT);
        return volume;
    }

    private static boolean hasContainingVolume(Config config, String path) {
        if (config.volumes == null) return false;
        for (Config.VolumeDescriptor volume : config.volumes) {
            if (volume != null && supportsMetadata(volume) && containsPath(volume.baseDir, path)) {
                return true;
            }
        }
        return false;
    }

    private static Config.VolumeDescriptor matchingVolume(Config config, String path) {
        if (config.volumes == null) return null;
        Config.VolumeDescriptor result = null;
        for (Config.VolumeDescriptor volume : config.volumes) {
            if (volume != null && sameStorageEndpoint(volume.baseDir, path)) {
                if (result != null) {
                    if (!sameCredentialsAndOptions(result, volume)) {
                        throw new IllegalArgumentException(
                                "fixed snapshot manifest has multiple matching storage credential "
                                        + "routes; configure a volume whose root contains "
                                        + path);
                    }
                    continue;
                }
                result = volume;
            }
        }
        return result;
    }

    private static boolean supportsMetadata(Config.VolumeDescriptor volume) {
        return volume.kinds != null && volume.kinds.contains(Config.VolumeUsageKind.META);
    }

    private static boolean sameCredentialsAndOptions(
            Config.VolumeDescriptor left, Config.VolumeDescriptor right) {
        return Objects.equals(left.accessId, right.accessId)
                && Objects.equals(left.secretKey, right.secretKey)
                && Objects.equals(left.customOptions, right.customOptions);
    }

    private static boolean containsPath(String base, String path) {
        if (base == null || base.trim().isEmpty()) return false;
        try {
            URI baseUri = URI.create(base);
            URI pathUri = URI.create(path);
            if (isLocal(baseUri) && isLocal(pathUri)) {
                Path basePath = baseUri.getScheme() == null ? Paths.get(base) : Paths.get(baseUri);
                Path pathPath = pathUri.getScheme() == null ? Paths.get(path) : Paths.get(pathUri);
                return pathPath.toAbsolutePath()
                        .normalize()
                        .startsWith(basePath.toAbsolutePath().normalize());
            }
            String normalizedBase = trimTrailingSlash(base);
            String normalizedPath = trimTrailingSlash(path);
            return normalizedPath.equals(normalizedBase)
                    || normalizedPath.startsWith(normalizedBase + "/");
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static boolean samePath(String left, String right) {
        if (left == null || right == null) return false;
        try {
            Path leftLocal = Paths.get(left);
            Path rightLocal = Paths.get(right);
            if (leftLocal.isAbsolute() && rightLocal.isAbsolute()) {
                return leftLocal.normalize().equals(rightLocal.normalize());
            }
        } catch (IllegalArgumentException ignored) {
            // Compare URI-backed storage paths below.
        }
        try {
            URI leftUri = URI.create(left);
            URI rightUri = URI.create(right);
            if (isLocal(leftUri) && isLocal(rightUri)) {
                Path leftPath = leftUri.getScheme() == null ? Paths.get(left) : Paths.get(leftUri);
                Path rightPath =
                        rightUri.getScheme() == null ? Paths.get(right) : Paths.get(rightUri);
                return leftPath.toAbsolutePath()
                        .normalize()
                        .equals(rightPath.toAbsolutePath().normalize());
            }
            return trimTrailingSlash(left).equals(trimTrailingSlash(right));
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static boolean isSnapshotManifest(String path) {
        int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf(java.io.File.separatorChar));
        return path.substring(slash + 1).startsWith("SNAPSHOT-");
    }

    private static boolean sameStorageEndpoint(String base, String path) {
        if (base == null || base.trim().isEmpty()) return false;
        try {
            URI baseUri = URI.create(base);
            URI pathUri = URI.create(path);
            String baseScheme = baseUri.getScheme() == null ? "file" : baseUri.getScheme();
            String pathScheme = pathUri.getScheme() == null ? "file" : pathUri.getScheme();
            return baseScheme.equalsIgnoreCase(pathScheme)
                    && Objects.equals(baseUri.getAuthority(), pathUri.getAuthority());
        } catch (IllegalArgumentException ignored) {
            return false;
        }
    }

    private static boolean isLocal(URI uri) {
        return uri.getScheme() == null || "file".equalsIgnoreCase(uri.getScheme());
    }

    private static String manifestRoot(String manifest, String dbId) {
        try {
            URI uri = URI.create(manifest);
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                Path parent = Paths.get(uri).getParent();
                if (parent == null) throw new IllegalArgumentException("manifest has no parent");
                if (parent.getFileName() != null
                        && "snapshot".equals(parent.getFileName().toString())) {
                    parent = parent.getParent();
                }
                if (parent != null
                        && parent.getFileName() != null
                        && parent.getFileName().toString().equals(dbId)) {
                    parent = parent.getParent();
                }
                if (parent == null)
                    throw new IllegalArgumentException("manifest has no storage root");
                return parent.toAbsolutePath().normalize().toString();
            }
        } catch (IllegalArgumentException ignored) {
            // Fall through to a generic URI/path root.
        }
        int separator = manifest.lastIndexOf('/');
        if (separator <= 0) {
            throw new IllegalArgumentException("manifest has no parent: " + manifest);
        }
        String parent = manifest.substring(0, separator);
        if (parent.endsWith("/snapshot")) {
            separator = parent.lastIndexOf('/');
            if (separator <= 0) throw new IllegalArgumentException("manifest has no storage root");
            parent = parent.substring(0, separator);
        }
        if (parent.endsWith("/" + dbId)) {
            separator = parent.lastIndexOf('/');
            if (separator <= 0) throw new IllegalArgumentException("manifest has no storage root");
            parent = parent.substring(0, separator);
        }
        return parent + "/";
    }

    private static String trimTrailingSlash(String value) {
        while (value.endsWith("/") && value.length() > 1) {
            value = value.substring(0, value.length() - 1);
        }
        return value;
    }
}
