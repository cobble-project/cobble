package io.cobble;

import io.cobble.table.CatalogTable;
import io.cobble.table.DataField;
import io.cobble.table.FileCatalog;
import io.cobble.table.LogicalTypes;
import io.cobble.table.Table;
import io.cobble.table.TableIdentifier;
import io.cobble.table.TableKey;
import io.cobble.table.TableReader;
import io.cobble.table.TableScanCursor;
import io.cobble.table.TableScanPlan;
import io.cobble.table.TableScanSplit;
import io.cobble.table.TableSchema;
import io.cobble.table.TableSchemaChange;
import io.cobble.table.TableSnapshotCommitter;
import io.cobble.table.TableWritePlan;
import io.cobble.table.Value;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Cross-JVM table writer, reader, snapshot-commit, and typed split-scan coverage. */
public class TableCrossProcessTest {
    private static final int TOTAL_BUCKETS = 2;
    private static final int ROWS = 8192;
    private static final long VALUE_A = 100L;
    private static final long VALUE_B = 3_000_000_000L;
    private static final String CATALOG_STORAGE_ID = "cross-process-catalog";
    private static final TableIdentifier TABLE =
            new TableIdentifier(Collections.singletonList("cross"), "events");

    @TempDir Path root;
    private final List<Child> children = new ArrayList<Child>();

    @Test
    void tableLifecycleCrossesIndependentWriterReaderAndScanJvmProcesses() throws Exception {
        Path shared = Files.createDirectories(root.resolve("shared"));
        Path parentPrimary = Files.createDirectories(root.resolve("parent-primary"));
        Config parentConfig = runtimeConfig(shared, parentPrimary);
        Path planAFile = root.resolve("write-plan-a.ser");
        Path planBFile = root.resolve("write-plan-b.ser");
        Path writerA0 = root.resolve("writer-a0.ser");
        Path writerA1 = root.resolve("writer-a1.ser");
        Path writerB0 = root.resolve("writer-b0.ser");
        Path writerB1 = root.resolve("writer-b1.ser");
        Path readerReady = root.resolve("reader.ready");
        Path readerStaged = root.resolve("reader.staged");
        Path readerStagedAck = root.resolve("reader.staged-ack");
        Path readerGo = root.resolve("reader.go");
        Path readerReport = root.resolve("reader.ser");

        try (FileCatalog catalog = FileCatalog.open(parentConfig, CATALOG_STORAGE_ID)) {
            catalog.createNamespace(TABLE.namespace());
            try (CatalogTable tableA = catalog.createTable(TABLE, schema())) {
                writeObject(
                        planAFile, tableA.newWriteBuilder().totalBuckets(TOTAL_BUCKETS).build());

                Child firstWriter =
                        startWorker(
                                "writer",
                                shared,
                                root.resolve("writer-a0"),
                                planAFile,
                                0,
                                -1L,
                                VALUE_A,
                                false,
                                false,
                                writerA0);
                Child secondWriter =
                        startWorker(
                                "writer",
                                shared,
                                root.resolve("writer-a1"),
                                planAFile,
                                1,
                                -1L,
                                VALUE_A,
                                false,
                                false,
                                writerA1);
                awaitChild(firstWriter, "writer A0");
                awaitChild(secondWriter, "writer A1");
                WriterReport reportA0 = readObject(writerA0, WriterReport.class);
                WriterReport reportA1 = readObject(writerA1, WriterReport.class);
                assertDisjointAndComplete(reportA0, reportA1);

                GlobalSnapshot snapshotA;
                try (TableSnapshotCommitter committer = tableA.snapshotCommitter(parentConfig, 2)) {
                    assertNull(committer.submit(10L, reportA0.snapshot));
                    snapshotA = committer.submit(10L, reportA1.snapshot);
                    assertNotNull(snapshotA);
                }

                Child reader =
                        startReader(
                                shared,
                                root.resolve("reader-primary"),
                                snapshotA.id,
                                readerReady,
                                readerStaged,
                                readerStagedAck,
                                readerGo,
                                readerReport);
                awaitFile(readerReady, "reader readiness", reader);

                try (CatalogTable tableB =
                        catalog.evolveSchema(
                                TABLE,
                                Collections.singletonList(
                                        TableSchemaChange.alterFieldType(
                                                "score", LogicalTypes.int64())))) {
                    writeObject(
                            planBFile,
                            tableB.newWriteBuilder().totalBuckets(TOTAL_BUCKETS).build());

                    Child resumedWriter =
                            startWorker(
                                    "writer",
                                    shared,
                                    root.resolve("writer-a0"),
                                    planBFile,
                                    0,
                                    reportA0.snapshot.snapshotId,
                                    VALUE_B,
                                    true,
                                    true,
                                    writerB0);
                    Child resumedWriterTwo =
                            startWorker(
                                    "writer",
                                    shared,
                                    root.resolve("writer-a1"),
                                    planBFile,
                                    1,
                                    reportA1.snapshot.snapshotId,
                                    VALUE_B,
                                    true,
                                    true,
                                    writerB1);
                    awaitChild(resumedWriter, "writer B0");
                    awaitChild(resumedWriterTwo, "writer B1");
                    WriterReport reportB0 = readObject(writerB0, WriterReport.class);
                    WriterReport reportB1 = readObject(writerB1, WriterReport.class);
                    assertDisjointAndComplete(reportB0, reportB1);

                    Files.write(readerStaged, new byte[] {1});
                    awaitFile(readerStagedAck, "reader staged-snapshot check", reader);

                    GlobalSnapshot snapshotB;
                    try (TableSnapshotCommitter committer =
                            tableB.snapshotCommitter(parentConfig, 2)) {
                        snapshotB =
                                committer.commitBatch(
                                        20L, Arrays.asList(reportB0.snapshot, reportB1.snapshot));
                        assertNotNull(snapshotB);
                    }

                    Files.write(readerGo, new byte[] {1});
                    awaitChild(reader, "latest/fixed reader");
                    ReaderReport report = readObject(readerReport, ReaderReport.class);
                    assertEquals(snapshotA.id, report.fixedSnapshotId);
                    assertEquals(snapshotB.id, report.latestSnapshotId);
                    assertEquals(snapshotB.id, report.scanPlan.snapshotId());
                    assertEquals(
                            LogicalTypes.int64(),
                            report.scanPlan.schema().fields().get(1).logicalType());

                    Set<Long> observed = new HashSet<Long>();
                    List<TableScanSplit> splits = report.scanPlan.splits();
                    assertEquals(2, splits.size());
                    for (int index = 0; index < splits.size(); index++) {
                        Path splitFile = root.resolve("split-" + index + ".ser");
                        Path scanFile = root.resolve("scan-" + index + ".ser");
                        writeObject(splitFile, splits.get(index));
                        Child scanner =
                                startScanner(
                                        shared,
                                        root.resolve("scanner-" + index),
                                        splitFile,
                                        scanFile);
                        awaitChild(scanner, "typed scan " + index);
                        ScanReport scan = readObject(scanFile, ScanReport.class);
                        for (Row row : scan.rows) {
                            assertEquals(VALUE_B + row.id, row.score);
                            assertTrue(observed.add(row.id), "duplicate scanned key " + row.id);
                        }
                    }
                    assertEquals(expectedIds(), observed);
                }
            }
        } finally {
            terminateChildren();
        }
    }

    public static final class Worker {
        private Worker() {}

        public static void main(String[] args) throws Exception {
            String command = args[0];
            if ("writer".equals(command)) {
                writer(args);
            } else if ("reader".equals(command)) {
                reader(args);
            } else if ("scanner".equals(command)) {
                scanner(args);
            } else {
                throw new IllegalArgumentException("unknown worker command: " + command);
            }
        }

        private static void writer(String[] args) throws Exception {
            Path shared = path(args, 1);
            Path local = path(args, 2);
            TableWritePlan plan = readObject(path(args, 3), TableWritePlan.class);
            int bucket = Integer.parseInt(args[4]);
            long sourceSnapshotId = Long.parseLong(args[5]);
            long valueBase = Long.parseLong(args[6]);
            boolean resume = Boolean.parseBoolean(args[7]);
            boolean widened = Boolean.parseBoolean(args[8]);
            Path report = path(args, 9);
            Config config = runtimeConfig(shared, local);
            io.cobble.table.TableWriterBuilder builder = plan.writerBuilder(config).bucket(bucket);
            try (Table table =
                    resume ? builder.resumeFromSnapshot(sourceSnapshotId) : builder.open()) {
                Set<Long> ids = new HashSet<Long>();
                for (long id = 0L; id < ROWS; id++) {
                    TableKey key = table.keyBuilder().push(Value.int64(id)).build();
                    if (key.bucket() != bucket) continue;
                    table.put(
                            Arrays.asList(
                                    Value.int64(id),
                                    widened
                                            ? Value.int64(valueBase + id)
                                            : Value.int32((int) (valueBase + id))));
                    ids.add(id);
                }
                writeObject(report, new WriterReport(table.snapshot(), ids));
            }
        }

        private static void reader(String[] args) throws Exception {
            Path shared = path(args, 1);
            Path local = path(args, 2);
            long fixedSnapshotId = Long.parseLong(args[3]);
            Path ready = path(args, 4);
            Path staged = path(args, 5);
            Path stagedAck = path(args, 6);
            Path go = path(args, 7);
            Path report = path(args, 8);
            Config config = runtimeConfig(shared, local);
            try (FileCatalog catalog = FileCatalog.open(config, CATALOG_STORAGE_ID);
                    CatalogTable table = catalog.loadTable(TABLE);
                    TableReader latest =
                            table.readerBuilder(config).currentGlobalSnapshot().open();
                    TableReader fixed =
                            table.readerBuilder(config).globalSnapshot(fixedSnapshotId).open()) {
                assertRows(latest, VALUE_A, false);
                assertRows(fixed, VALUE_A, false);
                Files.write(ready, new byte[] {1});
                awaitSignal(staged, "parent staged B shards");
                assertRows(latest, VALUE_A, false);
                assertRows(fixed, VALUE_A, false);
                Files.write(stagedAck, new byte[] {1});
                awaitSignal(go, "parent B commit");
                assertRows(latest, VALUE_B, true);
                assertRows(fixed, VALUE_A, false);
                writeObject(
                        report, new ReaderReport(fixed.scanPlan().snapshotId(), latest.scanPlan()));
            }
        }

        private static void scanner(String[] args) throws Exception {
            Path shared = path(args, 1);
            Path local = path(args, 2);
            TableScanSplit split = readObject(path(args, 3), TableScanSplit.class);
            Path report = path(args, 4);
            List<Row> rows = new ArrayList<Row>();
            try (TableScanCursor cursor =
                    split.openTypedScanner(runtimeConfig(shared, local), 4096)) {
                List<Value> row;
                while ((row = cursor.nextRow()) != null) {
                    rows.add(
                            new Row(
                                    ((Number) row.get(0).raw()).longValue(),
                                    ((Number) row.get(1).raw()).longValue()));
                }
            }
            writeObject(report, new ScanReport(rows));
        }

        private static void assertRows(TableReader reader, long valueBase, boolean widened) {
            for (long id = 0L; id < ROWS; id++) {
                List<Value> row = reader.get(reader.keyBuilder().push(Value.int64(id)).build());
                if (!Arrays.asList(
                                Value.int64(id),
                                widened
                                        ? Value.int64(valueBase + id)
                                        : Value.int32((int) (valueBase + id)))
                        .equals(row)) {
                    throw new AssertionError("unexpected row for id " + id + ": " + row);
                }
            }
            if (!reader.schema()
                    .fields()
                    .get(1)
                    .logicalType()
                    .equals(widened ? LogicalTypes.int64() : LogicalTypes.int32())) {
                throw new AssertionError("unexpected score schema: " + reader.schema());
            }
        }
    }

    private Child startWorker(
            String command,
            Path shared,
            Path local,
            Path plan,
            int bucket,
            long sourceSnapshotId,
            long valueBase,
            boolean resume,
            boolean widened,
            Path report)
            throws IOException {
        return startChild(
                command,
                shared.toString(),
                local.toString(),
                plan.toString(),
                Integer.toString(bucket),
                Long.toString(sourceSnapshotId),
                Long.toString(valueBase),
                Boolean.toString(resume),
                Boolean.toString(widened),
                report.toString());
    }

    private Child startReader(
            Path shared,
            Path local,
            long fixedSnapshotId,
            Path ready,
            Path staged,
            Path stagedAck,
            Path go,
            Path report)
            throws IOException {
        return startChild(
                "reader",
                shared.toString(),
                local.toString(),
                Long.toString(fixedSnapshotId),
                ready.toString(),
                staged.toString(),
                stagedAck.toString(),
                go.toString(),
                report.toString());
    }

    private Child startScanner(Path shared, Path local, Path split, Path report)
            throws IOException {
        return startChild(
                "scanner",
                shared.toString(),
                local.toString(),
                split.toString(),
                report.toString());
    }

    private Child startChild(String command, String... args) throws IOException {
        List<String> commandLine = new ArrayList<String>();
        commandLine.add(javaExecutable());
        // Child JVMs do not inherit Surefire's module access flags. Java 8 has no modules.
        if (!System.getProperty("java.specification.version").startsWith("1.")) {
            commandLine.add("--add-exports=java.base/sun.nio.ch=ALL-UNNAMED");
        }
        String profile = nativeProfile();
        if (profile != null) commandLine.add("-Dcobble.native.profile=" + profile);
        commandLine.add("-cp");
        commandLine.add(testClasspath());
        commandLine.add(Worker.class.getName());
        commandLine.add(command);
        commandLine.addAll(Arrays.asList(args));
        Path log = root.resolve("child-" + children.size() + "-" + command + ".log");
        ProcessBuilder builder = new ProcessBuilder(commandLine);
        if (profile != null) builder.environment().put("COBBLE_NATIVE_PROFILE", profile);
        builder.redirectErrorStream(true);
        builder.redirectOutput(log.toFile());
        Child child = new Child(builder.start(), log);
        children.add(child);
        return child;
    }

    private void awaitChild(Child child, String description) throws Exception {
        assertTrue(
                child.process.waitFor(60L, TimeUnit.SECONDS),
                description + " did not exit before deadline; log:\n" + readChildLog(child));
        assertEquals(
                0,
                child.process.exitValue(),
                description + " failed; log:\n" + readChildLog(child));
    }

    private void awaitFile(Path file, String description, Child child) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
        while (!Files.exists(file) && System.nanoTime() < deadline) {
            if (!child.process.isAlive()) {
                throw new AssertionError(
                        description + " ended before signal; log:\n" + readChildLog(child));
            }
            Thread.sleep(10L);
        }
        assertTrue(
                Files.exists(file),
                description + " did not arrive before deadline; log:\n" + readChildLog(child));
    }

    private static void awaitSignal(Path file, String description) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60L);
        while (!Files.exists(file) && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
        if (!Files.exists(file)) {
            throw new AssertionError(description + " did not arrive before deadline");
        }
    }

    private String readChildLog(Child child) throws IOException {
        return Files.exists(child.log)
                ? new String(Files.readAllBytes(child.log), StandardCharsets.UTF_8)
                : "<missing log: " + child.log + ">";
    }

    private void terminateChildren() throws InterruptedException {
        for (Child child : children) {
            if (!child.process.isAlive()) continue;
            child.process.destroy();
            if (!child.process.waitFor(5L, TimeUnit.SECONDS)) {
                child.process.destroyForcibly();
                child.process.waitFor(5L, TimeUnit.SECONDS);
            }
        }
    }

    private static void assertDisjointAndComplete(WriterReport first, WriterReport second) {
        Set<Long> ids = new HashSet<Long>(first.ids);
        assertTrue(Collections.disjoint(ids, second.ids));
        ids.addAll(second.ids);
        assertEquals(expectedIds(), ids);
    }

    private static Set<Long> expectedIds() {
        Set<Long> ids = new HashSet<Long>();
        for (long id = 0L; id < ROWS; id++) ids.add(id);
        return ids;
    }

    private static Config runtimeConfig(Path shared, Path primary) {
        Config config = new Config().totalBuckets(TOTAL_BUCKETS);
        Config.VolumeDescriptor metadata = new Config.VolumeDescriptor();
        metadata.baseDir = shared.toString();
        metadata.kinds =
                Arrays.asList(Config.VolumeUsageKind.META, Config.VolumeUsageKind.SNAPSHOT);
        Config.VolumeDescriptor data = new Config.VolumeDescriptor();
        data.baseDir = primary.toString();
        data.kinds = Collections.singletonList(Config.VolumeUsageKind.PRIMARY_DATA_PRIORITY_HIGH);
        config.addVolume(metadata).addVolume(data);
        config.reader = new Config.ReaderConfigEntry();
        config.reader.reloadToleranceSeconds = 0L;
        config.l0FileLimit = 10_000;
        return config;
    }

    private static TableSchema schema() {
        return new TableSchema(
                Arrays.asList(
                        new DataField(1L, "id", LogicalTypes.int64()),
                        new DataField(2L, "score", LogicalTypes.int32())),
                Collections.singletonList(1L),
                Collections.singletonList(1L));
    }

    private static void writeObject(Path file, Serializable value) throws IOException {
        try (ObjectOutputStream out =
                new ObjectOutputStream(new BufferedOutputStream(Files.newOutputStream(file)))) {
            out.writeObject(value);
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T readObject(Path file, Class<T> type)
            throws IOException, ClassNotFoundException {
        try (ObjectInputStream in =
                new ObjectInputStream(new BufferedInputStream(Files.newInputStream(file)))) {
            Object value = in.readObject();
            if (!type.isInstance(value)) throw new IOException("unexpected serialized payload");
            return (T) value;
        }
    }

    private static Path path(String[] args, int index) {
        return java.nio.file.Paths.get(args[index]);
    }

    private static String javaExecutable() {
        return java.nio.file.Paths.get(System.getProperty("java.home"), "bin", "java")
                .toAbsolutePath()
                .toString();
    }

    private static String testClasspath() {
        String surefire = System.getProperty("surefire.test.class.path");
        return surefire == null || surefire.isEmpty()
                ? System.getProperty("java.class.path")
                : surefire;
    }

    private static String nativeProfile() {
        String property = System.getProperty("cobble.native.profile");
        if (property != null && !property.trim().isEmpty()) return property;
        String environment = System.getenv("COBBLE_NATIVE_PROFILE");
        return environment == null || environment.trim().isEmpty() ? null : environment;
    }

    private static final class Child {
        final Process process;
        final Path log;

        Child(Process process, Path log) {
            this.process = process;
            this.log = log;
        }
    }

    private static final class WriterReport implements Serializable {
        private static final long serialVersionUID = 1L;
        final ShardSnapshot snapshot;
        final Set<Long> ids;

        WriterReport(ShardSnapshot snapshot, Set<Long> ids) {
            this.snapshot = snapshot;
            this.ids = ids;
        }
    }

    private static final class ReaderReport implements Serializable {
        private static final long serialVersionUID = 1L;
        final long fixedSnapshotId;
        final long latestSnapshotId;
        final TableScanPlan scanPlan;

        ReaderReport(long fixedSnapshotId, TableScanPlan scanPlan) {
            this.fixedSnapshotId = fixedSnapshotId;
            this.latestSnapshotId = scanPlan.snapshotId();
            this.scanPlan = scanPlan;
        }
    }

    private static final class ScanReport implements Serializable {
        private static final long serialVersionUID = 1L;
        final List<Row> rows;

        ScanReport(List<Row> rows) {
            this.rows = rows;
        }
    }

    private static final class Row implements Serializable {
        private static final long serialVersionUID = 1L;
        final long id;
        final long score;

        Row(long id, long score) {
            this.id = id;
            this.score = score;
        }
    }
}
