package io.cobble;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CustomFileSystemApiTest {
    @Test
    void fastCopyMethodsRemainOptionalInterfaceMethods() throws Exception {
        Method canFastCopyTo =
                CustomFileSystem.class.getMethod(
                        "canFastCopyTo", String.class, CustomFileSystem.class, String.class);
        Method fastCopyTo =
                CustomFileSystem.class.getMethod(
                        "fastCopyTo", String.class, CustomFileSystem.class, String.class);

        assertEquals(boolean.class, canFastCopyTo.getReturnType());
        assertEquals(void.class, fastCopyTo.getReturnType());
        assertTrue(canFastCopyTo.isDefault());
        assertTrue(fastCopyTo.isDefault());
    }
}
