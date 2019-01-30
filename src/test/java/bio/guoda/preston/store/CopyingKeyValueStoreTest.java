package bio.guoda.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

public class CopyingKeyValueStoreTest {

    @Test
    public void copyTo() throws IOException {
        KeyValueStore testSourceStore = new KeyValueStore() {
            @Override
            public void put(String key, String value) throws IOException {

            }

            @Override
            public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return "streaming-key";
            }

            @Override
            public void put(String key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(String key) throws IOException {
                return IOUtils.toInputStream(key + "-value", StandardCharsets.UTF_8);
            }
        };
        final Map<String, String> cache = new HashMap<>();

        CopyingKeyValueStore store = new CopyingKeyValueStore(testSourceStore, new KeyValueStore() {


            @Override
            public void put(String key, String value) throws IOException {
                cache.putIfAbsent(key, value);
            }

            @Override
            public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                String key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
                cache.put(key, IOUtils.toString(os.toByteArray(), StandardCharsets.UTF_8.name()));
                return key;
            }

            @Override
            public void put(String key, InputStream is) throws IOException {
                cache.putIfAbsent(key, IOUtils.toString(is, StandardCharsets.UTF_8));
            }

            @Override
            public InputStream get(String key) throws IOException {
                return IOUtils.toInputStream(cache.get(key), StandardCharsets.UTF_8);
            }
        });

        assertThat(cache.get("foo"), Is.is(not("foo-value")));

        InputStream barStream = store.get("foo");

        IOUtils.copy(barStream, new NullOutputStream());
        assertThat(cache.get("foo"), Is.is("foo-value"));


    }

}