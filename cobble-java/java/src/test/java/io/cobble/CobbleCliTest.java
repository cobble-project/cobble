package io.cobble;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link CobbleCli} and {@link CobbleCliProcess}. These exercise the bundled {@code
 * cobble-cli} executable built by the Maven {@code generate-resources} phase, so they require
 * {@code mvnw} to have produced the native artifacts (which the build does by default).
 */
class CobbleCliTest {
    private CobbleCliProcess liveProcess;

    @AfterEach
    void tearDown() {
        if (liveProcess != null) {
            liveProcess.close();
            liveProcess = null;
        }
    }

    @Test
    void extractsBundledCli() throws IOException {
        Path path = CobbleCli.extract(NativeProfile.RELEASE);
        assertTrue(Files.isRegularFile(path), "extracted cli should exist: " + path);
        assertTrue(Files.isExecutable(path), "extracted cli should be executable: " + path);
        // The release binary is the default profile, so it must be resolvable from resources.
        String resource =
                NativePlatform.resourceBase(NativeProfile.RELEASE)
                        + "/"
                        + NativePlatform.cliFileName();
        assertNotNull(
                CobbleCli.class.getClassLoader().getResource(resource),
                "missing bundled resource: " + resource);
    }

    @Test
    void extractCachesPerProfile() throws IOException {
        // The cache is keyed by profile: extracting debug then release (or vice versa) must yield
        // distinct paths pointing at the respective bundled binary, not a shared cached path.
        Path release1 = CobbleCli.extract(NativeProfile.RELEASE);
        Path debug = CobbleCli.extract(NativeProfile.DEBUG);
        Path release2 = CobbleCli.extract(NativeProfile.RELEASE);
        assertNotEquals(release1, debug, "debug and release paths must differ");
        assertEquals(release1, release2, "repeated release extract should hit the cache");
        assertTrue(Files.isExecutable(debug), "debug binary should be executable");
        assertTrue(Files.isExecutable(release1), "release binary should be executable");
    }

    @Test
    void runsHelpSuccessfully() throws IOException, InterruptedException {
        // cobble-cli prints usage to stderr and exits 0.
        int exit = CobbleCli.run(Collections.singletonList("--help"));
        assertEquals(0, exit);
    }

    @Test
    void runsRemoteCompactorHelpSuccessfully() throws IOException, InterruptedException {
        int exit = CobbleCli.run(Arrays.asList("remote-compactor", "--help"));
        assertEquals(0, exit);
    }

    @Test
    void nonZeroExitRaisesTypedException() {
        // web-monitor without --config exits non-zero.
        CobbleCliException ex =
                assertThrows(
                        CobbleCliException.class,
                        () -> CobbleCli.run(Collections.singletonList("web-monitor")));
        assertTrue(ex.exitCode() != 0, "exit code should be non-zero");
        String stderr = ex.stderr();
        assertNotNull(stderr);
        assertTrue(
                stderr.contains("web-monitor requires --config"),
                "stderr should mention the missing arg: " + stderr);
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void runInterruptedTerminatesChildAndPreservesInterrupt() throws Exception {
        // run() blocks on a long-lived server. Interrupting the caller must: terminate the child
        // (so the drain threads unblock), propagate InterruptedException, and set the interrupt
        // flag.
        int port = findFreePort();
        AtomicReference<Throwable> caught = new AtomicReference<>();
        AtomicBoolean interruptFlag = new AtomicBoolean(false);
        Thread runner =
                new Thread(
                        () -> {
                            try {
                                CobbleCli.run(
                                        CobbleCli.remoteCompactorArgs(null, "127.0.0.1:" + port));
                            } catch (Throwable t) {
                                // Capture the interrupt status immediately, while still on the
                                // runner thread: it is unreliable to read from a dead thread.
                                interruptFlag.set(Thread.currentThread().isInterrupted());
                                caught.set(t);
                            }
                        });
        runner.start();
        // Wait for the server to bind, then interrupt the blocked run().
        assertTrue(awaitPortOpen("127.0.0.1", port, 15_000), "server should bind before interrupt");
        runner.interrupt();
        runner.join(30_000);
        assertFalse(runner.isAlive(), "run() thread should have terminated after interrupt");
        Throwable t = caught.get();
        assertNotNull(t, "run() should have thrown");
        assertTrue(t instanceof InterruptedException, "expected InterruptedException, got " + t);
        // run() re-interrupts the calling thread before propagating.
        assertTrue(
                interruptFlag.get(),
                "interrupt status should be preserved on the runner after run() throws");
        // The bound port should be released once the child is destroyed.
        assertTrue(awaitPortClosed("127.0.0.1", port, 10_000), "child should be terminated");
    }

    @Test
    void respectsCobbleCliPathOverride() throws Exception {
        Path bundled = CobbleCli.extract(NativeProfile.RELEASE);
        Path copy = Files.createTempFile("cobble-cli-override-", NativePlatform.cliFileName());
        try {
            Files.copy(bundled, copy, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            copy.toFile().setExecutable(true);
            Properties backup = setSystemProperty(CobbleCli.CLI_PATH_PROPERTY, copy.toString());
            try {
                Path resolved = CobbleCli.extract(NativeProfile.RELEASE);
                assertEquals(copy.toAbsolutePath(), resolved.toAbsolutePath());
                // The override should actually run.
                int exit = CobbleCli.run(Collections.singletonList("--help"));
                assertEquals(0, exit);
            } finally {
                restoreSystemProperty(backup);
            }
        } finally {
            Files.deleteIfExists(copy);
        }
    }

    @Test
    void missingOverridePathThrowsInsteadOfFallingBack() throws Exception {
        Path nonexistent = Paths.get("/definitely/not/a/real/cobble-cli-" + System.nanoTime());
        Properties backup = setSystemProperty(CobbleCli.CLI_PATH_PROPERTY, nonexistent.toString());
        try {
            IOException ex =
                    assertThrows(IOException.class, () -> CobbleCli.extract(NativeProfile.RELEASE));
            assertTrue(
                    ex.getMessage().contains("does not exist"),
                    "message should mention the missing path: " + ex.getMessage());
        } finally {
            restoreSystemProperty(backup);
        }
    }

    @Test
    void commandBuilderAllowsOutputRedirection() throws Exception {
        ProcessBuilder builder =
                CobbleCli.command(NativeProfile.RELEASE, Collections.singletonList("--help"));
        builder.redirectErrorStream(true);
        Path stdout = Files.createTempFile("cobble-cli-stdout-", ".txt");
        builder.redirectOutput(stdout.toFile());
        try {
            Process process = builder.start();
            assertTrue(process.waitFor(30, TimeUnit.SECONDS), "process should exit promptly");
            String output = new String(Files.readAllBytes(stdout), StandardCharsets.UTF_8);
            assertTrue(
                    output.contains("Usage:"), "captured output should contain usage: " + output);
        } finally {
            Files.deleteIfExists(stdout);
        }
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void closeTerminatesLongRunningProcess() throws Exception {
        int port = findFreePort();
        liveProcess = CobbleCli.startRemoteCompactor(null, "127.0.0.1:" + port);
        assertTrue(liveProcess.isAlive(), "remote-compactor should be running");
        // Wait for the server to bind by polling the TCP port. The CLI buffers its log lines when
        // stderr is not a TTY, so the address cannot reliably be parsed from logs.
        assertTrue(
                awaitPortOpen("127.0.0.1", port, 15_000), "remote-compactor should bind the port");
        assertTrue(liveProcess.isAlive(), "remote-compactor should still be running after bind");
        long start = System.currentTimeMillis();
        liveProcess.close();
        long elapsed = System.currentTimeMillis() - start;
        // close() must terminate the process, and do so within the grace period plus a margin.
        assertFalse(liveProcess.isAlive(), "remote-compactor should be terminated after close()");
        assertTrue(
                elapsed < CobbleCliProcess.DESTROY_GRACE_MILLIS + 5_000,
                "close() should terminate promptly, took " + elapsed + "ms");
    }

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void remoteCompactorBindsAndAcceptsConnections() throws Exception {
        int port = findFreePort();
        liveProcess = CobbleCli.startRemoteCompactor(null, "127.0.0.1:" + port);
        assertTrue(
                awaitPortOpen("127.0.0.1", port, 15_000), "remote-compactor should bind the port");
        // The remote compaction server is a raw TCP listener; a successful TCP connect proves the
        // bundled process is serving on the port we requested.
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress("127.0.0.1", port), 5_000);
            assertTrue(
                    socket.isConnected(),
                    "should connect to remote-compactor at 127.0.0.1:" + port);
        }
    }

    @Test
    void webMonitorArgsAreBuiltCorrectly() {
        Path cfg = Paths.get("/tmp/cobble-test.json");
        assertEquals(
                Arrays.asList(
                        "web-monitor",
                        "--config",
                        cfg.toAbsolutePath().toString(),
                        "--bind",
                        "127.0.0.1:9090"),
                CobbleCli.webMonitorArgs(cfg, "127.0.0.1:9090"));
        assertEquals(
                Arrays.asList("web-monitor", "--config", cfg.toAbsolutePath().toString()),
                CobbleCli.webMonitorArgs(cfg, null));
    }

    @Test
    void remoteCompactorArgsOmitNullConfig() {
        assertEquals(
                Arrays.asList("remote-compactor", "--bind", "127.0.0.1:0"),
                CobbleCli.remoteCompactorArgs(null, "127.0.0.1:0"));
        assertEquals(
                Collections.singletonList("remote-compactor"),
                CobbleCli.remoteCompactorArgs(null, null));
    }

    /** Binds and immediately closes a server socket to reserve a transient free TCP port. */
    private static int findFreePort() throws IOException {
        try (java.net.ServerSocket socket = new java.net.ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    /**
     * Polls until a TCP connection to {@code host:port} succeeds, or the deadline passes. Used
     * instead of parsing the server's stderr, since the CLI buffers log output when stderr is not a
     * TTY.
     */
    private static boolean awaitPortOpen(String host, int port, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                return true;
            } catch (IOException e) {
                Thread.sleep(50);
            }
        }
        return false;
    }

    /**
     * Polls until a TCP connection to {@code host:port} fails (i.e. the server has released the
     * port), or the deadline passes.
     */
    private static boolean awaitPortClosed(String host, int port, long timeoutMillis)
            throws InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMillis;
        while (System.currentTimeMillis() < deadline) {
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500);
                Thread.sleep(50);
            } catch (IOException e) {
                return true;
            }
        }
        return false;
    }

    /**
     * Sets a system property and returns a backup marker. The marker records the previous value (or
     * absence) so {@link #restoreSystemProperty(Properties)} can fully revert it, including
     * clearing a property that did not previously exist.
     */
    private static Properties setSystemProperty(String key, String value) {
        Properties backup = new Properties();
        if (System.getProperty(key) != null) {
            backup.setProperty(key, System.getProperty(key));
        } else {
            backup.setProperty("__absent__" + key, "true");
        }
        System.setProperty(key, value);
        return backup;
    }

    private static void restoreSystemProperty(Properties backup) {
        for (String name : backup.stringPropertyNames()) {
            if (name.startsWith("__absent__")) {
                System.clearProperty(name.substring("__absent__".length()));
            } else {
                System.setProperty(name, backup.getProperty(name));
            }
        }
    }
}
