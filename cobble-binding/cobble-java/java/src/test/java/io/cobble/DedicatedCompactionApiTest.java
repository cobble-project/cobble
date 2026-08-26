package io.cobble;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class DedicatedCompactionApiTest {
    @Test
    void monitorPlansAndExecutorRunsIndependently() throws Exception {
        Path root = Files.createTempDirectory("cobble-java-dedicated-");
        Config config = dedicatedConfig(root);
        byte[] value = new byte[1024];
        try (Db db = Db.open(config)) {
            for (int i = 0; i < 40; i++) {
                db.put(0, String.format("key-%08d", i).getBytes("UTF-8"), 0, value);
            }

            Path dbPath = root.resolve(db.id());
            try (DedicatedCompactionMonitor monitor =
                            DedicatedCompactionMonitor.watchDatabases(
                                    config, Collections.singletonList(dbPath.toString()));
                    DedicatedCompactionExecutor executor =
                            DedicatedCompactionExecutor.open(config)) {
                assertEquals(
                        DedicatedCompactionExecutor.Outcome.RESULT_PUBLISHED,
                        executeStablePlan(monitor, executor, db.id()));
                assertArrayEquals(value, db.get(0, "key-00000000".getBytes("UTF-8"), 0));
            }
        }
    }

    private static DedicatedCompactionExecutor.Outcome executeStablePlan(
            DedicatedCompactionMonitor monitor, DedicatedCompactionExecutor executor, String dbId)
            throws Exception {
        for (int attempt = 0; attempt < 10; attempt++) {
            DedicatedCompactionPlan plan = waitForPlan(monitor);
            DedicatedCompactionPlan decoded = DedicatedCompactionPlan.decode(plan.encode());
            assertEquals(dbId, decoded.dbId());
            assertEquals(plan.jobId(), decoded.jobId());
            DedicatedCompactionExecutor.Outcome outcome = executor.execute(decoded);
            if (outcome != DedicatedCompactionExecutor.Outcome.STALE) {
                return outcome;
            }
        }
        throw new AssertionError("a stable compaction plan was not produced before timeout");
    }

    private static Config dedicatedConfig(Path root) {
        Config config = new Config().addVolume(root.toString()).numColumns(1).totalBuckets(1);
        config.memtableCapacity = 8 * 1024;
        config.memtableBufferCount = 2;
        config.l0FileLimit = 2;
        config.baseFileSize = 4 * 1024;
        config.l1BaseBytes = 8 * 1024;
        config.levelSizeMultiplier = 2;
        config.maxLevel = 4;
        config.blockCacheSize = 0;
        config.compactionMode = Config.CompactionMode.DEDICATED;
        config.runtimeManifestMode = Config.RuntimeManifestMode.AUTO;
        config.compactionDedicatedPollIntervalMs = 50L;
        config.compactionOrphanMinAgeMs = 30_000L;
        return config;
    }

    private static DedicatedCompactionPlan waitForPlan(DedicatedCompactionMonitor monitor)
            throws Exception {
        long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
        while (System.nanoTime() < deadline) {
            List<DedicatedCompactionPlan> plans = monitor.poll();
            if (!plans.isEmpty()) {
                assertFalse(plans.get(0).jobId().isEmpty());
                return plans.get(0);
            }
            Thread.sleep(50L);
        }
        throw new AssertionError("dedicated compaction plan was not produced before timeout");
    }
}
