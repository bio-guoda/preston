package bio.guoda.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import bio.guoda.preston.model.RefNodeFactory;
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
import static org.junit.Assert.assertThat;
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
            IRI key = blobStore.putBlob(is);
            assertThat(wasClosed.get(), is(expectedClosingStream));
            assertThat(key.getIRIString(), is("hash://sha256/b822f1cd2dcfc685b47e83e3980289fd5d8e3ff3a82def24d7d1d68bb272eb32"));
            InputStream inputStream = blobStore.get(key);
            assertThat(TestUtil.toUTF8(inputStream), is("testing123"));
        }

        assertTrue(wasClosed.get());
    }

    @Test
    public void putURI() throws IOException {
        BlobStore blobStore = new BlobStoreAppendOnly(getTestPersistence());
        IRI key = blobStore.putBlob(RefNodeFactory.toIRI("pesto:123"));
        assertThat(key.getIRIString(), is("hash://sha256/02707ce2db146bfe983e40cca527240cd46b6e8723710757c4c24f0d2adb8b7c"));
        InputStream inputStream = blobStore.get(key);
        assertThat(TestUtil.toUTF8(inputStream), is("pesto:123"));
    }

    @Test
    public void putBlank() throws IOException {
        BlobStore blobStore = new BlobStoreAppendOnly(getTestPersistence());
        BlankNode entity = RefNodeFactory.toBlank();
        IRI key = blobStore.putBlob(entity);
        InputStream inputStream = blobStore.get(key);
        entity.uniqueReference();
        assertThat(TestUtil.toUTF8(inputStream), is("_:" + entity.uniqueReference()));
    }

    static KeyValueStore getTestPersistence() {
        return new KeyValueStore() {
            private final Map<String, String> lookup = new TreeMap<>();

            @Override
            public void put(String key, String value) throws IOException {
                if (lookup.containsKey(key) && !value.equals(lookup.get(key))) {
                    throw new IOException("can't overwrite with value [" + value + "]");
                }
                lookup.putIfAbsent(key, value);
            }

            @Override
            public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                String key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
                ByteArrayInputStream bais = new ByteArrayInputStream(os.toByteArray());
                put(key, bais);
                return key;
            }

            @Override
            public void put(String key, InputStream is) throws IOException {
                lookup.putIfAbsent(key, TestUtil.toUTF8(is));
            }

            @Override
            public InputStream get(String key) throws IOException {
                String input = lookup.get(key);
                return input == null ? null : IOUtils.toInputStream(input, StandardCharsets.UTF_8);
            }
        };
    }


}
