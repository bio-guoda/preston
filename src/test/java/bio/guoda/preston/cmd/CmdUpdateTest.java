package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertThat;

public class CmdUpdateTest {

    private static final AtomicInteger putAttemptCount = new AtomicInteger(0);

    @Test
    public void doUpdate() {
        assertThat(putAttemptCount.get(), Is.is(0));
        new CmdUpdate().run(
                new BlobStoreNull(),
                new StatementStoreNull());
        assertThat(putAttemptCount.get() > 0, Is.is(true));
    }

    private static class BlobStoreNull implements BlobStore {


        @Override
        public IRI put(InputStream is) throws IOException {
            putAttemptCount.incrementAndGet();
            return null;
        }

        @Override
        public InputStream get(IRI key) throws IOException {
            return null;
        }
    }

    private static class StatementStoreNull implements StatementStore {
        @Override
        public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {

        }

        @Override
        public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
            return null;
        }
    }
}