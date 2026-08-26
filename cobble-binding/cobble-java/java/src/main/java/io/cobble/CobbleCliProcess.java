package io.cobble;

import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A handle to a running {@code cobble-cli} process started via {@link CobbleCli#start}.
 *
 * <p>The {@code cobble-cli} {@code remote-compactor} and {@code web-monitor} subcommands are
 * long-lived servers. This handle keeps a reference to the underlying {@link Process} and
 * implements {@link AutoCloseable} so callers can manage the lifecycle in a {@code try}
 * -with-resources block. On {@link #close()} the process is asked to terminate gracefully ({@link
 * Process#destroy}) and, if it does not exit within a short grace period, is killed forcibly
 * ({@link Process#destroyForcibly}).
 *
 * <p>This class does <em>not</em> drain the child's stdout/stderr. The convenience {@code start*}
 * methods in {@link CobbleCli} inherit output to the parent by default; callers that need custom
 * redirects should build a {@link ProcessBuilder} via {@link CobbleCli#command} and launch it with
 * {@link CobbleCli#start(ProcessBuilder)}.
 */
public final class CobbleCliProcess implements AutoCloseable {
    private static final Logger LOG = Logger.getLogger(CobbleCliProcess.class.getName());

    /** Grace period (milliseconds) given to the process to exit after {@code destroy}. */
    static final long DESTROY_GRACE_MILLIS = 3000L;

    private final Process process;
    private final String commandLine;

    CobbleCliProcess(Process process, String commandLine) {
        this.process = process;
        this.commandLine = commandLine;
    }

    /** The underlying JVM {@link Process}. */
    public Process process() {
        return process;
    }

    /** A human-readable representation of the command line that was launched. */
    public String commandLine() {
        return commandLine;
    }

    /** Whether the process is still alive. */
    public boolean isAlive() {
        return process.isAlive();
    }

    /**
     * Blocks until the process terminates and returns its exit code.
     *
     * @throws InterruptedException if the current thread is interrupted while waiting.
     */
    public int waitFor() throws InterruptedException {
        return process.waitFor();
    }

    /**
     * Blocks until the process terminates or the timeout elapses.
     *
     * @return the exit code, or {@code -1} if the process did not terminate in time (consistent
     *     with {@link Process#waitFor(long, TimeUnit)} returning {@code false}).
     */
    public int waitFor(long timeout, TimeUnit unit) throws InterruptedException {
        boolean exited = process.waitFor(timeout, unit);
        return exited ? process.exitValue() : -1;
    }

    @Override
    public void close() {
        if (!process.isAlive()) {
            return;
        }
        process.destroy();
        try {
            if (!process.waitFor(DESTROY_GRACE_MILLIS, TimeUnit.MILLISECONDS)) {
                LOG.warning(
                        "cobble-cli did not exit within "
                                + DESTROY_GRACE_MILLIS
                                + "ms, forcibly destroying: "
                                + commandLine);
                process.destroyForcibly();
                process.waitFor();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            process.destroyForcibly();
        } catch (RuntimeException e) {
            LOG.log(Level.WARNING, "Failed to stop cobble-cli process: " + commandLine, e);
            try {
                process.destroyForcibly();
            } catch (RuntimeException ignored) {
                // Best effort.
            }
        }
    }
}
