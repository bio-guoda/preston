package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;

public class ArchivingLoggerTest {

    private BlobStoreNull fooBarringBlobStore = new BlobStoreNull() {
        AtomicInteger provCounter = new AtomicInteger(0);

        @Override
        public IRI put(InputStream is) throws IOException {
            super.put(is);
            return RefNodeFactory.toIRI("foo:bar:" + provCounter.getAndIncrement());
        }

    };

    @Test
    public void appendToRoot() throws IOException {
        HexaStoreNull provIndex = new HexaStoreNull();
        ArchivingLogger logger = new ArchivingLogger(
                new PersistingLocal() {

                },
                fooBarringBlobStore,
                provIndex,
                new ActivityContext() {
                    @Override
                    public IRI getActivity() {
                        return null;
                    }

                    @Override
                    public String getDescription() {
                        return null;
                    }
                });

        IRI fooBar = RefNodeFactory.toIRI("foo:bar");
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(0));
        logger.start();
        logger.on(RefNodeFactory.toStatement(fooBar, fooBar, fooBar));
        logger.stop();
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(1));
        assertThat(provIndex.queryAndValuePairs.size(), Is.is(1));
        Pair<RDFTerm, RDFTerm> query = provIndex.queryAndValuePairs.get(0).getKey();
        RDFTerm value = provIndex.queryAndValuePairs.get(0).getValue();
        assertThat(query.getKey(), Is.is(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH));
        assertThat(query.getValue(), Is.is(RefNodeConstants.HAS_VERSION));
        assertThat(value, Is.is(Is.is(RefNodeFactory.toIRI("foo:bar:0"))));

    }

    @Test
    public void appendToRootTwice() throws IOException {
        HexaStoreNull provIndex = new HexaStoreInMemory();
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(0));
        append(provIndex, new PersistingLocal());
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(1));
        append(provIndex, new PersistingLocal());
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(2));
        assertThat(provIndex.queryAndValuePairs.size(), Is.is(2));
        Pair<RDFTerm, RDFTerm> query = provIndex.queryAndValuePairs.get(0).getKey();
        RDFTerm value = provIndex.queryAndValuePairs.get(0).getValue();
        assertThat(query.getKey(), Is.is(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH));
        assertThat(query.getValue(), Is.is(RefNodeConstants.HAS_VERSION));
        assertThat(value, Is.is(Is.is(RefNodeFactory.toIRI("foo:bar:0"))));

        Pair<RDFTerm, RDFTerm> query1 = provIndex.queryAndValuePairs.get(1).getKey();
        RDFTerm value1 = provIndex.queryAndValuePairs.get(1).getValue();
        assertThat(query1.getKey(), Is.is(RefNodeConstants.HAS_PREVIOUS_VERSION));
        assertThat(query1.getValue(), Is.is(RefNodeFactory.toIRI("foo:bar:0")));
        assertThat(value1, Is.is(Is.is(RefNodeFactory.toIRI("foo:bar:1"))));

    }

    @Test
    public void appendToAnchor() throws IOException {
        HexaStoreNull provIndex = new HexaStoreNull();
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(0));
        appendWithAnchor(provIndex);
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(1));
        Pair<RDFTerm, RDFTerm> query = provIndex.queryAndValuePairs.get(0).getKey();
        RDFTerm value = provIndex.queryAndValuePairs.get(0).getValue();
        assertThat(query.getKey(), Is.is(RefNodeConstants.HAS_PREVIOUS_VERSION));
        assertThat(query.getValue(), Is.is(RefNodeFactory.toIRI("some:anchor")));
        assertThat(value, Is.is(Is.is(RefNodeFactory.toIRI("foo:bar:0"))));

    }

    @Test
    public void appendToAnchorTwice() throws IOException {
        HexaStoreNull provIndex = new HexaStoreInMemory();
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(0));
        appendWithAnchor(provIndex);
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(1));
        appendWithAnchor(provIndex);
        assertThat(provIndex.putLogVersionAttemptCount.get(), Is.is(2));
        Pair<RDFTerm, RDFTerm> query = provIndex.queryAndValuePairs.get(0).getKey();
        RDFTerm value = provIndex.queryAndValuePairs.get(0).getValue();
        assertThat(query.getKey(), Is.is(RefNodeConstants.HAS_PREVIOUS_VERSION));
        assertThat(query.getValue(), Is.is(RefNodeFactory.toIRI("some:anchor")));
        assertThat(value, Is.is(Is.is(RefNodeFactory.toIRI("foo:bar:0"))));

        Pair<RDFTerm, RDFTerm> query1 = provIndex.queryAndValuePairs.get(1).getKey();
        RDFTerm value1 = provIndex.queryAndValuePairs.get(1).getValue();
        assertThat(query1.getKey(), Is.is(RefNodeConstants.HAS_PREVIOUS_VERSION));
        assertThat(query1.getValue(), Is.is(RefNodeFactory.toIRI("foo:bar:0")));
        assertThat(value1, Is.is(Is.is(RefNodeFactory.toIRI("foo:bar:1"))));

    }

    private void appendWithAnchor(HexaStoreNull provIndex) throws IOException {
        PersistingLocal persistingLocal = new PersistingLocal() {

        };
        persistingLocal.setProvenanceArchor(RefNodeFactory.toIRI("some:anchor"));
        append(provIndex, persistingLocal);
    }

    private void append(HexaStoreNull provIndex, PersistingLocal persistingLocal) throws IOException {
        ArchivingLogger logger = new ArchivingLogger(
                persistingLocal,
                fooBarringBlobStore,
                provIndex,
                new ActivityContext() {
                    @Override
                    public IRI getActivity() {
                        return null;
                    }

                    @Override
                    public String getDescription() {
                        return null;
                    }
                });

        IRI fooBar = RefNodeFactory.toIRI("foo:bar");
        logger.start();
        logger.on(RefNodeFactory.toStatement(fooBar, fooBar, fooBar));
        logger.stop();
    }

}