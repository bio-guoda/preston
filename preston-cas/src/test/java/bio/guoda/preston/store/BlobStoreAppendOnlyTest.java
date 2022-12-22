package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class BlobStoreAppendOnlyTest {

    @Test
    public void put() throws IOException {
        assertClosingStream(false);
    }

    @Test
    public void putAndClose() throws IOException {
        assertClosingStream(true);
    }

    private void assertClosingStream(boolean expectedClosingStream) throws IOException {
        BlobStore blobStore = new BlobStoreAppendOnly(
                TestUtil.getTestPersistence(),
                expectedClosingStream,
                HashType.sha256
        );
        AtomicBoolean wasClosed = new AtomicBoolean(false);
        InputStream testing123 = IOUtils.toInputStream("testing123", StandardCharsets.UTF_8);

        InputStream wrappedInputStream = new InputStream() {

            @Override
            public int read() throws IOException {
                return testing123.read();
            }

            @Override
            public void close() throws IOException {
                wasClosed.set(true);
                super.close();
            }
        };

        try (InputStream is = wrappedInputStream) {
            assertThat(wasClosed.get(), is(false));
            IRI key = blobStore.put(is);
            assertThat(wasClosed.get(), is(expectedClosingStream));
            assertThat(key.getIRIString(), is("hash://sha256/b822f1cd2dcfc685b47e83e3980289fd5d8e3ff3a82def24d7d1d68bb272eb32"));
            InputStream inputStream = blobStore.get(key);
            assertThat(TestUtil.toUTF8(inputStream), is("testing123"));
        }

        assertTrue(wasClosed.get());
    }


}
