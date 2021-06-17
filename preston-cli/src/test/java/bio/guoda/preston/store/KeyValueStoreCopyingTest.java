package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyValueStoreCopyingTest {

    @Test
    public void copyTo() throws IOException {
        KeyValueStore testSourceStore = new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return RefNodeFactory.toIRI("streaming-key");
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return IOUtils.toInputStream(key + "-value", StandardCharsets.UTF_8);
            }
        };
        final Map<String, String> cache = new HashMap<>();

        KeyValueStoreCopying store = new KeyValueStoreCopying(testSourceStore, new KeyValueStore() {


            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                ByteArrayOutputStream os = new ByteArrayOutputStream();
                IRI key = keyGeneratingStream.generateKeyWhileStreaming(is, os);
                String value = IOUtils.toString(os.toByteArray(), StandardCharsets.UTF_8.name());
                cache.putIfAbsent(key.getIRIString(), value);
                return key;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                cache.putIfAbsent(key.getIRIString(), IOUtils.toString(is, StandardCharsets.UTF_8));
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                String value = cache.get(key.getIRIString());
                return StringUtils.isBlank(value) ? null : IOUtils.toInputStream(value, StandardCharsets.UTF_8);
            }
        });

        assertThat(cache.get("foo"), Is.is(not("<foo>-value")));

        InputStream barStream = store.get(RefNodeFactory.toIRI("foo"));

        IOUtils.copy(barStream, NullOutputStream.NULL_OUTPUT_STREAM);
        assertThat(cache.get("foo"), Is.is("<foo>-value"));


    }

}