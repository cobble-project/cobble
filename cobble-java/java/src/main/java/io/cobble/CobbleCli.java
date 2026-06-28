package io.cobble;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Launches the bundled {@code cobble-cli} executable as a child process.
 *
 * <p>Users of the {@code io.github.cobble-project:cobble} artifact can start the remote compactor
 * and web monitor servers — or run any other {@code cobble-cli} subcommand — without installing
 * {@code cobble-cli} separately. The executable is extracted from the JAR's {@code native/}
 * resources into a temporary file (cached per JVM) and invoked via {@link ProcessBuilder}, which
 * keeps the CLI's server lifecycle, stdout/stderr, exit codes, and signal handling in a dedicated
 * process rather than embedding the Tokio runtime into the JVM.
 *
 * <p>The profile (debug/release) is selected the same way as the JNI library: the {@code
 * cobble.native.profile} system property, falling back to the {@code COBBLE_NATIVE_PROFILE}
 * environment variable, defaulting to {@code release}.
 *
 * <p>Override the executable location with the {@code cobble.cli.path} system property or the
 * {@code COBBLE_CLI_PATH} environment variable (the property takes precedence). When set, the
 * bundled resource is not extracted.
 */
public final class CobbleCli {
    private static final Logger LOG = Logger.getLogger(CobbleCli.class.getName());

    /** System property overriding the path to the {@code cobble-cli} executable. */
    public static final String CLI_PATH_PROPERTY = "cobble.cli.path";

    /** Environment variable overriding the path to the {@code cobble-cli} executable. */
    public static final String CLI_PATH_ENV = "COBBLE_CLI_PATH";

    private static final Object EXTRACT_LOCK = new Object();

    /** Cached extracted executable paths, keyed by profile so debug/release never collide. */
    private static final Map<NativeProfile, Path> extractedCliPaths =
            new EnumMap<>(NativeProfile.class);

    /**
     * Upper bound on the number of bytes captured from a child stream by {@link #run}. Beyond this
     * the stream is still drained (to avoid blocking the child) but no longer appended, so a
     * runaway process cannot exhaust heap. 1 MiB is ample for the CLI's help/error output.
     */
    static final int MAX_CAPTURE_BYTES = 1 << 20;

    private CobbleCli() {}

    /**
     * Extracts the bundled {@code cobble-cli} executable for the current platform/profile to a
     * temporary file (cached per JVM) and returns its path.
     *
     * @param profile the build profile to extract (debug/release).
     * @return the absolute path to the extracted executable.
     * @throws IOException if the resource is missing or cannot be written.
     */
    public static Path extract(NativeProfile profile) throws IOException {
        Path override = resolveOverride();
        if (override != null) {
            if (!Files.isExecutable(override)) {
                throw new IOException("Configured cobble-cli path is not executable: " + override);
            }
            return override;
        }
        synchronized (EXTRACT_LOCK) {
            Path cached = extractedCliPaths.get(profile);
            if (cached != null && Files.isExecutable(cached)) {
                return cached;
            }
            String resource =
                    NativePlatform.resourceBase(profile) + "/" + NativePlatform.cliFileName();
            LOG.info(
                    "Extracting cobble-cli from resource " + resource + " with profile " + profile);
            try (InputStream in = CobbleCli.class.getClassLoader().getResourceAsStream(resource)) {
                if (in == null) {
                    throw new IOException("Missing cobble-cli resource: " + resource);
                }
                Path target = Files.createTempFile("cobble-cli-", NativePlatform.cliFileName());
                try {
                    Files.copy(in, target, StandardCopyOption.REPLACE_EXISTING);
                    makeExecutable(target);
                    target.toFile().deleteOnExit();
                } catch (IOException e) {
                    deleteQuietly(target);
                    throw e;
                }
                extractedCliPaths.put(profile, target);
                LOG.info("Extracted cobble-cli to " + target);
                return target;
            }
        }
    }

    /**
     * Builds a {@link ProcessBuilder} configured to run {@code cobble-cli} with the given
     * arguments. The executable is resolved via {@link #extract(NativeProfile)} using the active
     * profile. Callers may further customize the returned builder (redirects, environment, etc.).
     *
     * @param profile the build profile to use.
     * @param args the CLI arguments (subcommand and flags), e.g. {@code ["remote-compactor",
     *     "--config", path, "--bind", addr]}.
     */
    public static ProcessBuilder command(NativeProfile profile, List<String> args)
            throws IOException {
        Path cli = extract(profile);
        List<String> command = new ArrayList<>();
        command.add(cli.toAbsolutePath().toString());
        if (args != null) {
            command.addAll(args);
        }
        return new ProcessBuilder(command);
    }

    /**
     * Starts {@code cobble-cli} with the given arguments using the active profile and returns a
     * handle that manages the process lifecycle.
     *
     * <p>The child's stdout/stderr are redirected to the parent process via {@link
     * ProcessBuilder.Redirect#INHERIT} so the CLI's logs are visible and cannot block on a full OS
     * pipe buffer. For full control over output (capture, redirect to file, discard, etc.) use
     * {@link #command(NativeProfile, List)} to build a {@link ProcessBuilder} and {@link
     * #start(ProcessBuilder)} to launch it.
     */
    public static CobbleCliProcess start(List<String> args) throws IOException {
        return start(activeProfile(), args);
    }

    /** Variant of {@link #start(List)} that selects the build profile explicitly. */
    public static CobbleCliProcess start(NativeProfile profile, List<String> args)
            throws IOException {
        ProcessBuilder builder = command(profile, args);
        builder.redirectOutput(ProcessBuilder.Redirect.INHERIT);
        builder.redirectError(ProcessBuilder.Redirect.INHERIT);
        return start(builder);
    }

    /**
     * Starts {@code cobble-cli} from a fully-configured {@link ProcessBuilder}. The builder must
     * already specify the executable and arguments (typically obtained from {@link #command}); the
     * caller controls output redirects and environment. This is the entry point to use when
     * capturing stdout/stderr or redirecting to files.
     */
    public static CobbleCliProcess start(ProcessBuilder builder) throws IOException {
        Process process;
        try {
            process = builder.start();
        } catch (IOException e) {
            throw new IOException("Failed to start cobble-cli: " + builder.command(), e);
        }
        return new CobbleCliProcess(process, String.join(" ", builder.command()));
    }

    /**
     * Runs {@code cobble-cli} with the given arguments and blocks until it exits.
     *
     * <p>Both stdout and stderr are drained asynchronously so the child cannot block on a full OS
     * pipe. stdout is discarded; stderr is captured (bounded by {@link #MAX_CAPTURE_BYTES}) and
     * attached to {@link CobbleCliException} on non-zero exit.
     *
     * @return the process exit code.
     * @throws IOException if the process cannot be started or its output cannot be read.
     * @throws InterruptedException if the current thread is interrupted while waiting.
     * @throws CobbleCliException if the process exits with a non-zero status.
     */
    public static int run(List<String> args) throws IOException, InterruptedException {
        return run(activeProfile(), args);
    }

    /** Variant of {@link #run(List)} that selects the build profile explicitly. */
    public static int run(NativeProfile profile, List<String> args)
            throws IOException, InterruptedException {
        ProcessBuilder builder = command(profile, args);
        CobbleCliProcess handle = start(builder);
        Process process = handle.process();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();
        // stdout is drained to a throwaway sink so the child cannot block on a full pipe; its
        // contents are intentionally discarded. stderr is captured (bounded) for error reporting.
        Thread stdoutDrain =
                drainAsync(process.getInputStream(), NullOutputStream.INSTANCE, MAX_CAPTURE_BYTES);
        Thread stderrDrain = drainAsync(process.getErrorStream(), stderr, MAX_CAPTURE_BYTES);
        int exitCode;
        try {
            exitCode = process.waitFor();
        } catch (InterruptedException e) {
            // The waiting thread was interrupted while the child may still be alive. Destroy the
            // child first so its stdout/stderr reach EOF and the drain threads below can complete;
            // otherwise join() would block forever. Then preserve the interrupt status and
            // propagate.
            handle.close();
            joinQuietly(stdoutDrain);
            joinQuietly(stderrDrain);
            Thread.currentThread().interrupt();
            throw e;
        }
        joinQuietly(stdoutDrain);
        joinQuietly(stderrDrain);
        handle.close();
        if (exitCode != 0) {
            throw new CobbleCliException(
                    exitCode,
                    "cobble-cli exited with code " + exitCode,
                    stderr.toString(StandardCharsets.UTF_8.name()));
        }
        return exitCode;
    }

    /**
     * Convenience entry point: starts {@code cobble-cli remote-compactor} with the given config and
     * bind address. Either argument may be {@code null}, in which case the corresponding flag is
     * omitted and {@code cobble-cli} applies its own defaults (config defaults; bind derived from
     * config or {@code 127.0.0.1:0}).
     */
    public static CobbleCliProcess startRemoteCompactor(Path config, String bindAddr)
            throws IOException {
        return start(remoteCompactorArgs(config, bindAddr));
    }

    /**
     * Convenience entry point: starts {@code cobble-cli web-monitor} with the given config and bind
     * address. {@code config} is required by {@code web-monitor}; {@code bindAddr} may be {@code
     * null}.
     */
    public static CobbleCliProcess startWebMonitor(Path config, String bindAddr)
            throws IOException {
        return start(webMonitorArgs(config, bindAddr));
    }

    /** Builds the argument list for {@code remote-compactor}. */
    static List<String> remoteCompactorArgs(Path config, String bindAddr) {
        List<String> args = new ArrayList<>();
        args.add("remote-compactor");
        if (config != null) {
            args.add("--config");
            args.add(config.toAbsolutePath().toString());
        }
        if (bindAddr != null && !bindAddr.isEmpty()) {
            args.add("--bind");
            args.add(bindAddr);
        }
        return args;
    }

    /** Builds the argument list for {@code web-monitor}. */
    static List<String> webMonitorArgs(Path config, String bindAddr) {
        List<String> args = new ArrayList<>();
        args.add("web-monitor");
        if (config != null) {
            args.add("--config");
            args.add(config.toAbsolutePath().toString());
        }
        if (bindAddr != null && !bindAddr.isEmpty()) {
            args.add("--bind");
            args.add(bindAddr);
        }
        return args;
    }

    /** Resolves the active build profile from the property/env override, defaulting to release. */
    static NativeProfile activeProfile() {
        String raw = System.getProperty("cobble.native.profile");
        if (isBlank(raw)) {
            raw = System.getenv("COBBLE_NATIVE_PROFILE");
        }
        return NativeProfile.fromString(raw);
    }

    /**
     * Returns the configured override path for the executable, or {@code null} if no override is
     * configured.
     *
     * @throws IOException if an override is configured but points to a non-existent path.
     */
    static Path resolveOverride() throws IOException {
        String raw = System.getProperty(CLI_PATH_PROPERTY);
        if (isBlank(raw)) {
            raw = System.getenv(CLI_PATH_ENV);
        }
        if (isBlank(raw)) {
            return null;
        }
        Path path = Paths.get(raw);
        if (!Files.exists(path)) {
            throw new IOException("Configured cobble-cli path does not exist: " + path);
        }
        return path;
    }

    /**
     * Spawns a daemon thread that copies {@code in} into {@code sink} until EOF, stopping further
     * appends once {@code maxBytes} have been captured. The drain prevents the child from blocking
     * on a full OS pipe; the returned thread should be {@link Thread#join joined} after the process
     * exits.
     */
    private static Thread drainAsync(InputStream in, OutputStream sink, int maxBytes) {
        Thread thread =
                new Thread(
                        () -> {
                            byte[] buffer = new byte[4096];
                            int captured = 0;
                            int read;
                            try {
                                while ((read = in.read(buffer)) != -1) {
                                    int toCopy = Math.min(read, maxBytes - captured);
                                    if (toCopy > 0) {
                                        sink.write(buffer, 0, toCopy);
                                        captured += toCopy;
                                    }
                                }
                            } catch (IOException e) {
                                // Stream closed when the process exits; not an error.
                                LOG.log(Level.FINE, "cobble-cli stream drain ended", e);
                            }
                        },
                        "cobble-cli-stream-drain");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    /**
     * Joins a drain thread, restoring the interrupt status if the join itself is interrupted. The
     * drain threads have already (or will soon) reach EOF once the child is destroyed, so this
     * should return promptly; it never throws.
     */
    private static void joinQuietly(Thread thread) {
        try {
            thread.join();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void makeExecutable(Path target) throws IOException {
        try {
            Files.setPosixFilePermissions(target, PosixFilePermissions.fromString("rwxr-xr-x"));
        } catch (UnsupportedOperationException e) {
            // Non-POSIX filesystem (e.g. Windows): executable bit is not applicable.
            if (!target.toFile().setExecutable(true)) {
                LOG.warning(
                        "Could not mark cobble-cli as executable on a non-POSIX filesystem: "
                                + target);
            }
        }
    }

    private static void deleteQuietly(Path path) {
        try {
            Files.deleteIfExists(path);
        } catch (IOException e) {
            LOG.log(Level.WARNING, "Failed to delete temporary cobble-cli: " + path, e);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
