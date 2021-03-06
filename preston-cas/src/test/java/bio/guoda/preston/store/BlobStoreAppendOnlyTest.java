package bio.guoda.preston.store;

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
        BlobStore blobStore = new BlobStoreAppendOnly(getTestPersistence(), expectedClosingStream);
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

    static KeyValueStore getTestPersistence() {
        return new KeyValueStore() {
            private final Map<String, String> lookup = new TreeMap<>();

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                IRI key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
                ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray());
                put(key, bais);
                return key;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                lookup.putIfAbsent(key.getIRIString(), TestUtil.toUTF8(is));
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                String input = lookup.get(key.getIRIString());
                return input == null ? null : IOUtils.toInputStream(input, StandardCharsets.UTF_8);
            }
        };
    }


}
