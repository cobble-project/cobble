package io.cobble;

import java.util.Collections;
import java.util.Map;

/** Request payload used by process-level custom filesystem registry callbacks. */
public final class ProcessFileSystemRequest {
    private final String baseDir;
    private final String normalizedBaseDir;
    private final String accessId;
    private final String secretKey;
    private final Map<String, String> customOptions;
    private final String builtinError;

    public ProcessFileSystemRequest(
            String baseDir,
            String normalizedBaseDir,
            String accessId,
            String secretKey,
            Map<String, String> customOptions,
            String builtinError) {
        this.baseDir = baseDir;
        this.normalizedBaseDir = normalizedBaseDir;
        this.accessId = accessId;
        this.secretKey = secretKey;
        this.customOptions = customOptions == null ? Collections.emptyMap() : customOptions;
        this.builtinError = builtinError;
    }

    public String baseDir() {
        return baseDir;
    }

    /**
     * Normalized URI form after native normalization, or {@code null} when normalization failed.
     */
    public String normalizedBaseDir() {
        return normalizedBaseDir;
    }

    /** Access id configured for the target volume, if present. */
    public String accessId() {
        return accessId;
    }

    /** Secret key configured for the target volume, if present. */
    public String secretKey() {
        return secretKey;
    }

    /** Custom key-value options configured for the target volume. */
    public Map<String, String> customOptions() {
        return customOptions;
    }

    /** Built-in resolver error message that triggered this fallback request. */
    public String builtinError() {
        return builtinError;
    }
}
