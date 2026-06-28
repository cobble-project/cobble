package io.cobble;

/**
 * Raised when a {@code cobble-cli} process exits with a non-zero status code.
 *
 * <p>Wrapper errors are categorized across distinct exception types so callers can distinguish
 * them:
 *
 * <ul>
 *   <li>Missing bundled resource / extraction failure / not executable — surfaced as {@link
 *       java.io.IOException} from {@link CobbleCli#extract} (with a descriptive message).
 *   <li>Process cannot be started — {@link java.io.IOException} from {@link CobbleCli#start}.
 *   <li>Process exited non-zero — this {@code CobbleCliException}.
 * </ul>
 */
public final class CobbleCliException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    private final int exitCode;
    private final String stderr;

    public CobbleCliException(int exitCode, String message, String stderr) {
        super(message);
        this.exitCode = exitCode;
        this.stderr = stderr;
    }

    /** The non-zero exit code returned by the {@code cobble-cli} process. */
    public int exitCode() {
        return exitCode;
    }

    /** The process's stderr output captured at failure time (may be empty). */
    public String stderr() {
        return stderr;
    }
}
