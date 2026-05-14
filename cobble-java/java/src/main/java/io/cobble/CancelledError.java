package io.cobble;

/** Raised when an async snapshot is cancelled before manifest publication completes. */
public final class CancelledError extends IllegalStateException {
    public CancelledError(String message) {
        super(message);
    }
}
