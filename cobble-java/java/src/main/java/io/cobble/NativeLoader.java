package io.cobble;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class for loading the native library. It extracts the library from the JAR resources and
 * loads it into the JVM.
 */
public final class NativeLoader {
    private static final Logger LOG = Logger.getLogger(NativeLoader.class.getName());
    private static final Object LOCK = new Object();
    private static NativeProfile loadedProfile;

    private NativeLoader() {}

    public static void load() {
        String profileRaw = System.getProperty("cobble.native.profile");
        if (isBlank(profileRaw)) {
            profileRaw = System.getenv("COBBLE_NATIVE_PROFILE");
        }
        load(NativeProfile.fromString(profileRaw));
    }

    public static void load(NativeProfile profile) {
        synchronized (LOCK) {
            if (loadedProfile != null) {
                if (loadedProfile != profile) {
                    throw new IllegalStateException(
                            "Native library already loaded with profile "
                                    + loadedProfile
                                    + ", cannot switch to "
                                    + profile);
                }
                return;
            }

            String resourcePath = resolveLibraryResourcePath(profile);
            LOG.info(
                    "Loading cobble native library from resource "
                            + resourcePath
                            + " with profile "
                            + profile);
            try (InputStream in =
                    NativeLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
                if (in == null) {
                    LOG.severe("Missing native library resource: " + resourcePath);
                    throw new IllegalStateException(
                            "Missing native library resource: " + resourcePath);
                }
                String suffix = resourcePath.substring(resourcePath.lastIndexOf('.'));
                Path extracted = Files.createTempFile("cobble-jni-", suffix);
                extracted.toFile().deleteOnExit();
                Files.copy(in, extracted, StandardCopyOption.REPLACE_EXISTING);
                System.load(extracted.toAbsolutePath().toString());
                loadedProfile = profile;
                String version = Utils.versionString();
                String commit = Utils.buildCommitId();
                LOG.info(
                        "Loaded cobble native library successfully. version="
                                + version
                                + " build_commit="
                                + commit);
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed to extract native library: " + resourcePath, e);
                throw new IllegalStateException(
                        "Failed to extract native library: " + resourcePath, e);
            } catch (RuntimeException e) {
                LOG.log(Level.SEVERE, "Failed to load native library: " + resourcePath, e);
                throw e;
            }
        }
    }

    static String resolveLibraryResourcePath(NativeProfile profile) {
        String os = detectOs();
        String arch = detectArch();
        return "native/"
                + os
                + "-"
                + arch
                + "/"
                + profile.resourceSegment()
                + "/"
                + detectLibraryFileName();
    }

    private static String detectOs() {
        String osName = System.getProperty("os.name", "").toLowerCase();
        if (osName.contains("mac") || osName.contains("darwin")) {
            return "macos";
        }
        if (osName.contains("win")) {
            return "windows";
        }
        if (osName.contains("nix") || osName.contains("nux") || osName.contains("linux")) {
            return "linux";
        }
        throw new IllegalStateException("Unsupported OS: " + osName);
    }

    private static String detectArch() {
        String arch = System.getProperty("os.arch", "").toLowerCase();
        if (arch.equals("x86_64") || arch.equals("amd64")) {
            return "x86_64";
        }
        if (arch.equals("aarch64") || arch.equals("arm64")) {
            return "aarch64";
        }
        throw new IllegalStateException("Unsupported architecture: " + arch);
    }

    private static String detectLibraryFileName() {
        String os = detectOs();
        switch (os) {
            case "macos":
                return "libcobblejni.dylib";
            case "linux":
                return "libcobblejni.so";
            case "windows":
                return "cobblejni.dll";
            default:
                throw new IllegalStateException("Unsupported OS: " + os);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }
}
