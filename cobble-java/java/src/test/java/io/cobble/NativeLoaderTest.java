package io.cobble;

import org.junit.jupiter.api.Test;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class NativeLoaderTest {
    @Test
    void packagesBothDebugAndReleaseNativeLibraries() {
        String debugPath = NativeLoader.resolveLibraryResourcePath(NativeProfile.DEBUG);
        String releasePath = NativeLoader.resolveLibraryResourcePath(NativeProfile.RELEASE);

        URL debugResource = NativeLoader.class.getClassLoader().getResource(debugPath);
        URL releaseResource = NativeLoader.class.getClassLoader().getResource(releasePath);

        assertNotNull(debugResource, "missing debug resource: " + debugPath);
        assertNotNull(releaseResource, "missing release resource: " + releasePath);
    }

    @Test
    void nativeUtilsExposeVersionAndCommit() {
        NativeLoader.load();
        String version = Utils.versionString();
        String commit = Utils.buildCommitId();
        assertNotNull(version);
        assertTrue(version.startsWith("v"), "version should start with v");
        assertNotNull(commit);
        assertFalse(commit.trim().isEmpty(), "commit should not be blank");
    }

    @Test
    void dbAndStructuredDbExposeNativeMethods() {
        assertNotNull(Db.class);
        assertNotNull(SingleDb.class);
        assertNotNull(StructuredDb.class);
        assertNotNull(ReadOnlyDb.class);
        assertNotNull(Reader.class);
        assertNotNull(DbCoordinator.class);
        assertNotNull(ShardSnapshot.class);
        assertNotNull(GlobalSnapshot.class);
        assertNotNull(ScanOptions.class);
        assertNotNull(ReadOptions.class);
        assertNotNull(ScanCursor.class);
        assertNotNull(ScanBatch.class);
    }

    @Test
    void nativeObjectCloseIsIdempotent() {
        AtomicInteger disposedCount = new AtomicInteger(0);
        NativeObject object =
                new NativeObject(1L) {
                    @Override
                    protected void disposeInternal(long nativeHandle) {
                        disposedCount.incrementAndGet();
                    }
                };
        object.close();
        object.close();
        assertTrue(object.isDisposed());
        assertEquals(1, disposedCount.get());
    }
}
