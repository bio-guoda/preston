package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.TreeMap;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsNull.nullValue;

public class KeyValueStoreProtocolAwareTest {

    @Test
    public void testTwoPrefixes() throws IOException {
        KeyValueStoreReadOnly store = new KeyValueStoreProtocolAware(new TreeMap<String, KeyValueStoreReadOnly>() {{
            put("foo", getTestStore("foo-store"));
            put("bar", getTestStore("bar-store"));
        }});

        assertThat(IOUtils.toString(store.get(RefNodeFactory.toIRI("foo:bla")), StandardCharsets.UTF_8), Is.is("foo:bla:content:from:foo-store"));
        assertThat(IOUtils.toString(store.get(RefNodeFactory.toIRI("bar:bla")), StandardCharsets.UTF_8), Is.is("bar:bla:content:from:bar-store"));
        assertThat(store.get(RefNodeFactory.toIRI("zoo:bla")), Is.is(nullValue()));
    }

    @Test
    public void testEmptyPrefixes() throws IOException {
        KeyValueStoreReadOnly store = new KeyValueStoreProtocolAware(new TreeMap<String, KeyValueStoreReadOnly>() {{
            put("", getTestStore("foo-store"));
        }});

        assertThat(IOUtils.toString(store.get(RefNodeFactory.toIRI("foo:bla")), StandardCharsets.UTF_8), Is.is("foo:bla:content:from:foo-store"));
        assertThat(IOUtils.toString(store.get(RefNodeFactory.toIRI("bar:bla")), StandardCharsets.UTF_8), Is.is("bar:bla:content:from:foo-store"));
    }

    @Test
    public void testOverlappingPrefixes() throws IOException {
        KeyValueStoreReadOnly store = new KeyValueStoreProtocolAware(new HashMap<String, KeyValueStoreReadOnly>() {{
            put("foo", getTestStore("foo-store"));
            put("f", getTestStore("f-store"));
        }});

        assertThat(IOUtils.toString(store.get(RefNodeFactory.toIRI("foo:bla")), StandardCharsets.UTF_8), Is.is("foo:bla:content:from:f-store"));
    }

    @Test
    public void testNoPrefixes() throws IOException {
        KeyValueStoreReadOnly store = new KeyValueStoreProtocolAware(new TreeMap<>());

        assertThat(store.get(RefNodeFactory.toIRI("foo:bla")), Is.is(nullValue()));
        assertThat(store.get(RefNodeFactory.toIRI("zoo:bla")), Is.is(nullValue()));
    }

    private KeyValueStoreReadOnly getTestStore(final String storeName) {
        return key -> IOUtils.toInputStream(key.getIRIString() + ":content:from:" + storeName, StandardCharsets.UTF_8);
    }


}