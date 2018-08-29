package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

public class TestUtil {
    public static String toUTF8(InputStream content) throws IOException {
        return content == null ? null : IOUtils.toString(content, StandardCharsets.UTF_8);
    }

    public static Persistence getTestPersistence() {
        return AppendOnlyBlobStoreTest.getTestPersistence();
    }
}
