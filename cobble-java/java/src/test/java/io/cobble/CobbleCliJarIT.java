package io.cobble;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Post-package integration test that runs the shaded JAR via {@code java -jar}.
 *
 * <p>This is the one test that exercises the full {@code java -jar cobble.jar <args>} entry point
 * end-to-end: the shaded manifest's {@code Main-Class} ({@link Main}), {@link Main} delegating to
 * {@link CobbleCli}, the bundled {@code cobble-cli} resource path inside the packaged JAR, and the
 * extraction of that executable to a temp file. The {@code CobbleCliTest} suite covers the wrapper
 * API against the classpath resources, but only this test proves the packaged artifact is
 * self-launchable.
 *
 * <p>Runs via {@code maven-failsafe-plugin} in the {@code integration-test} phase, after the shade
 * plugin has produced {@code target/cobble-<version>.jar}. The JAR path is injected through the
 * {@code cobble.test.jar} system property (set by the plugin configuration from
 * {@code ${project.build.directory}/${project.build.finalName}.jar}).
 */
class CobbleCliJarIT {

    /**
     * {@code java -jar <shaded.jar> --help} must exit 0 and print the CLI usage banner. This
     * single assertion covers: the manifest {@code Main-Class} resolves, {@link Main} forwards
     * args to {@link CobbleCli}, the bundled {@code cobble-cli} resource is found and extracted,
     * and the child process runs and returns its exit code.
     */
    @Test
    void javaJarRunsCliAndExitsZero() throws Exception {
        Path jar = resolveJar();
        List<String> command = new ArrayList<>();
        command.add(resolveJavaExecutable());
        command.add("-Dcobble.native.profile=release");
        command.add("-jar");
        command.add(jar.toAbsolutePath().toString());
        command.add("--help");

        ProcessBuilder builder = new ProcessBuilder(command);
        builder.redirectErrorStream(true);
        Process process = builder.start();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        drainTo(process.getInputStream(), out);
        boolean exited = process.waitFor(60, TimeUnit.SECONDS);
        String output = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertTrue(exited, "java -jar did not exit within 60s; output:\n" + output);
        int exitCode = process.exitValue();
        assertEquals(0, exitCode, "java -jar --help should exit 0; output:\n" + output);
        assertTrue(
                output.contains("Usage:"),
                "java -jar --help output should contain 'Usage:':\n" + output);
    }

    /** Resolves the shaded JAR from the injected property, falling back to the conventional path. */
    private static Path resolveJar() throws IOException {
        String configured = System.getProperty("cobble.test.jar");
        if (configured != null && !configured.isEmpty()) {
            Path p = Paths.get(configured);
            assertTrue(Files.isRegularFile(p), "configured JAR not found: " + p);
            return p;
        }
        // Fallback for IDE runs: target/<finalName>.jar relative to the module dir.
        Path moduleDir = Paths.get(".").toAbsolutePath();
        String version = readVersion(moduleDir);
        Path fallback = moduleDir.resolve("target").resolve("cobble-" + version + ".jar");
        assertTrue(
                Files.isRegularFile(fallback),
                "shaded JAR not found (run 'mvn package' first): " + fallback);
        return fallback;
    }

    /** Reads the project version from the pom next to the module, for the IDE fallback path. */
    private static String readVersion(Path moduleDir) throws IOException {
        Path pom = moduleDir.resolve("pom.xml");
        assertTrue(Files.isRegularFile(pom), "pom.xml not found at " + pom);
        String content = new String(Files.readAllBytes(pom), StandardCharsets.UTF_8);
        // The pom sets <version>0.2.0-SNAPSHOT</version> as the second child of <project>.
        int start = content.indexOf("<version>");
        int end = content.indexOf("</version>");
        if (start < 0 || end < 0 || end <= start) {
            throw new IOException("could not parse <version> from " + pom);
        }
        return content.substring(start + "<version>".length(), end).trim();
    }

    private static String resolveJavaExecutable() {
        String javaHome = System.getProperty("java.home");
        Path java = Paths.get(javaHome, "bin", "java");
        assertTrue(Files.isExecutable(java), "java executable not found at " + java);
        return java.toAbsolutePath().toString();
    }

    private static void drainTo(InputStream in, ByteArrayOutputStream sink) throws IOException {
        byte[] buffer = new byte[4096];
        int read;
        while ((read = in.read(buffer)) != -1) {
            sink.write(buffer, 0, read);
        }
    }
}
