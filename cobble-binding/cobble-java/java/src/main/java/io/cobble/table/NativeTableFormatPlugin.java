package io.cobble.table;

import io.cobble.Config;
import io.cobble.GlobalSnapshot;
import io.cobble.Reader;

import java.io.File;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/** Built-in adapter for snapshots written by Cobble's native typed table. */
public final class NativeTableFormatPlugin implements TableFormatPlugin {
    @Override
    public String formatId() {
        return TableMetadata.FORMAT;
    }

    @Override
    public TableFormatBinding bind(TableReadSnapshot snapshot) {
        if (!formatId().equals(snapshot.formatId())) {
            throw new IllegalArgumentException(
                    "native table plugin cannot bind format '" + snapshot.formatId() + "'");
        }
        if (snapshot.shards().isEmpty()) {
            throw new IllegalArgumentException("native table binding requires a global snapshot");
        }
        TableMetadata metadata = TableMetadata.fromJson(snapshot.shards().get(0).metadataJson());
        Table.Compiled compiled =
                Table.Compiled.from(metadata.schema(), snapshot.globalSnapshot().totalBuckets);
        return new NativeBinding(compiled);
    }

    @Override
    public Optional<TableReadSnapshot> resolvePath(Config config, TablePathRequest request)
            throws Exception {
        if (!configTargetsRequest(config, request.path())) {
            throw new IllegalArgumentException(
                    "table path request '"
                            + request.path()
                            + "' is not represented by the supplied read config volumes");
        }
        try (Reader reader = openReader(config, request)) {
            GlobalSnapshot snapshot = reader.currentGlobalSnapshot();
            if (snapshot == null) throw new TablePathMissingSnapshotException(request.path());
            return Optional.of(TableReadSnapshot.forGlobal(config, snapshot, request.tableName()));
        }
    }

    private static Reader openReader(Config config, TablePathRequest request) {
        try {
            return request.snapshotId() == null
                    ? Reader.openCurrent(config)
                    : Reader.open(config, request.snapshotId().longValue());
        } catch (IllegalStateException error) {
            if (request.snapshotId() == null
                    && "IO error: Global snapshot pointer missing".equals(error.getMessage())) {
                throw new TablePathMissingSnapshotException(request.path());
            }
            throw error;
        }
    }

    private static boolean configTargetsRequest(Config config, String path) {
        if (config.volumes == null || config.volumes.isEmpty()) return false;
        String normalized = normalize(path);
        for (Config.VolumeDescriptor volume : config.volumes) {
            if (volume != null && normalized.equals(normalize(volume.baseDir))) return true;
        }
        return false;
    }

    private static String normalize(String value) {
        if (value == null) return "";
        try {
            URI uri = URI.create(value);
            if (uri.getScheme() == null) uri = new File(value).toURI();
            if ("file".equalsIgnoreCase(uri.getScheme())) {
                return Paths.get(uri).toAbsolutePath().normalize().toUri().toString();
            }
            return uri.normalize().toString().replaceAll("/+$", "");
        } catch (IllegalArgumentException ignored) {
            return value.replaceAll("/+$", "");
        }
    }

    static final class NativeBinding implements TableFormatBinding {
        private final Table.Compiled compiled;
        private final TableReadSchema schema;
        private final List<DataField> keyFields;
        private final int[] physicalColumns;

        NativeBinding(Table.Compiled compiled) {
            this.compiled = compiled;
            this.schema = new TableReadSchema(compiled.schema.fields());
            List<DataField> keys = new ArrayList<DataField>();
            for (Long keyId : compiled.schema.primaryKey()) {
                for (DataField field : compiled.schema.fields()) {
                    if (keyId.longValue() == field.id()) {
                        keys.add(field);
                        break;
                    }
                }
            }
            this.keyFields = Collections.unmodifiableList(keys);
            this.physicalColumns = new int[compiled.physicalColumns];
            for (int index = 0; index < physicalColumns.length; index++) {
                physicalColumns[index] = index;
            }
        }

        @Override
        public TableReadSchema schema() {
            return schema;
        }

        @Override
        public List<DataField> keyFields() {
            return keyFields;
        }

        @Override
        public TableReadCapabilities capabilities() {
            return new TableReadCapabilities(true, true, false);
        }

        @Override
        public int[] physicalColumns() {
            return Arrays.copyOf(physicalColumns, physicalColumns.length);
        }

        @Override
        public TablePhysicalKey encodeKey(List<Value> values) {
            TableKeyBuilder builder = new TableKeyBuilder(compiled);
            for (Value value : values) builder.push(value);
            TableKey key = builder.build();
            return new TablePhysicalKey(key.bucket(), key.encodedInternal());
        }

        @Override
        public List<List<Value>> decode(int bucket, byte[] key, byte[][] columns) {
            List<Value> keys = KeyCodec.decodeOwned(compiled.keyTypes, ByteBuffer.wrap(key));
            return Collections.singletonList(Table.assembleRow(compiled, keys, columns));
        }

        @Override
        public List<List<Value>> decodeDirect(
                io.cobble.DirectScanEntry entry, int bucket, byte[] ownedPhysicalKey) {
            return Collections.singletonList(Table.decodeDirectScannedRowOwned(compiled, entry));
        }
    }
}
