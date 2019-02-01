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

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class BlobStoreAppendOnlyTest {

    @Test
    public void put() throws IOException {
        BlobStore blobStore = new BlobStoreAppendOnly(getTestPersistence());
        IRI key = blobStore.putBlob(IOUtils.toInputStream("testing123", StandardCharsets.UTF_8));
        InputStream inputStream = blobStore.get(key);
        assertThat(TestUtil.toUTF8(inputStream), is("testing123"));
    }

    @Test
    public void putURI() throws IOException {
        BlobStore blobStore = new BlobStoreAppendOnly(getTestPersistence());
        IRI key = blobStore.putBlob(RefNodeFactory.toIRI("pesto:123"));
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

    public static KeyValueStore getTestPersistence() {
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