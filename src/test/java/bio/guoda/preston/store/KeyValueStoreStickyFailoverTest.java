package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class KeyValueStoreStickyFailoverTest {

    @Test
    public void failover() throws IOException {
        KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(Arrays.asList(
                key -> {
                    throw new IOException("boom!");
                },
                key -> IOUtils.toInputStream("hello", StandardCharsets.UTF_8)
        ));

        assertHello(failover);
    }

    @Test
    public void failoverNulls() throws IOException {
        KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(Arrays.asList(
                key -> null,
                key -> null
        ));

        assertNull(failover.get(RefNodeFactory.toIRI("anything")));
    }

    @Test(expected = IOException.class)
    public void failoverExceptions() throws IOException {
        KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(Arrays.asList(
                key -> {
                    throw new IOException("boom!");
                },
                key -> {
                    throw new IOException("kaboom!");
                }
        ));

        assertHello(failover);
    }

    @Test
    public void stickyFailover() throws IOException {
        KeyValueStoreReadOnly keyValueAssertCalledOnceAndOnlyOnce = new KeyValueStoreReadOnly() {
            AtomicBoolean calledPrior = new AtomicBoolean(false);

            @Override
            public InputStream get(IRI key) throws IOException {
                assertFalse(calledPrior.get());
                calledPrior.set(true);
                throw new IOException("boom!");
            }
        };

        KeyValueStoreStickyFailover failover = new KeyValueStoreStickyFailover(Arrays.asList(
                keyValueAssertCalledOnceAndOnlyOnce,
                key -> IOUtils.toInputStream("hello", StandardCharsets.UTF_8)
        ));

        assertHello(failover);
        assertHello(failover);
    }

    public void assertHello(KeyValueStoreStickyFailover failover) throws IOException {
        InputStream inputStream = failover.get(RefNodeFactory.toIRI("something"));
        String actual = IOUtils.toString(inputStream, StandardCharsets.UTF_8);

        assertThat(actual, is("hello"));
    }

}