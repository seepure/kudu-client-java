package org.seepure.kudu.client.util;

public class AssertUtil {
    public static void assertTrue(boolean expression, String msg) {
        if (!expression) {
            throw new IllegalArgumentException(msg);
        }
    }
}
