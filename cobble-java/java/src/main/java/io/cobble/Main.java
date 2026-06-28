package io.cobble;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Executable entry point for the {@code cobble-java} JAR.
 *
 * <p>This lets the bundled {@code cobble-cli} be launched directly from Java, with no
 * {@code cobble-cli} binary installed separately:
 *
 * <pre>{@code
 * java -jar cobble.jar remote-compactor --config config.yaml --bind 127.0.0.1:18888
 * java -jar cobble.jar web-monitor --config config.yaml
 * }</pre>
 *
 * <p>Arguments are forwarded verbatim to {@code cobble-cli}. The executable is extracted from the
 * JAR's {@code native/} resources (cached per JVM) and run as a child process via {@link CobbleCli}.
 * The child's stdout/stderr are inherited so its logs are visible in the parent's console, and a
 * shutdown hook terminates the child gracefully when the JVM receives a signal (e.g. SIGINT/SIGTERM
 * or {@code kill <pid>}), so long-lived subcommands like {@code remote-compactor} shut down cleanly.
 *
 * <p>The JVM exits with the child's exit code. This mirrors running {@code cobble-cli} directly:
 * short-lived subcommands (e.g. {@code inspect}) return after completion, while server subcommands
 * block until the JVM is signaled.
 *
 * <p>With no arguments, prints the bundled {@code cobble-cli}'s own help (so {@code java -jar
 * cobble.jar} behaves like {@code cobble-cli} with no args).
 */
public final class Main {

    private Main() {}

    public static void main(String[] args) throws Exception {
        List<String> cliArgs = new ArrayList<>(Arrays.asList(args));

        CobbleCliProcess handle = CobbleCli.start(cliArgs);
        Runtime.getRuntime()
                .addShutdownHook(
                        new Thread(
                                () -> {
                                    // try-with-resources-style cleanup without throwing.
                                    try {
                                        handle.close();
                                    } catch (RuntimeException e) {
                                        // Best-effort during shutdown; the JVM is exiting anyway.
                                    }
                                },
                                "cobble-cli-shutdown"));

        // Block until the child exits. For server subcommands this returns only when the child is
        // killed (the shutdown hook then runs close() again, which is a no-op on a dead process).
        int exitCode = handle.waitFor();
        System.exit(exitCode);
    }
}
