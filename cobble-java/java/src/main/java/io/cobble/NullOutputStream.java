package io.cobble;

import java.io.OutputStream;

/**
 * An {@link OutputStream} that discards all bytes, equivalent to {@code /dev/null}. Used to drain a
 * child process's stdout/stderr without retaining it in memory.
 *
 * <p>This is a Java 8-compatible replacement for {@code ProcessBuilder.Redirect.DISCARD} (Java 9+)
 * and {@code OutputStream.nullOutputStream()} (Java 11+).
 */
final class NullOutputStream extends OutputStream {
    static final NullOutputStream INSTANCE = new NullOutputStream();

    private NullOutputStream() {}

    @Override
    public void write(int b) {
        // Discard.
    }

    @Override
    public void write(byte[] b, int off, int len) {
        // Discard.
    }
}
