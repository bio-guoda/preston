package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtilForProcessor;
import bio.guoda.preston.store.VersionedRDFChainEmitter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.MatcherAssert;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.MatcherAssert.assertThat;

public class VersionedRDFChainEmitterTest {

    private static final IRI SOME = RefNodeFactory.toIRI("http://some");
    private static final IRI OTHER = RefNodeFactory.toIRI("http://other");

    @Test
    public void replayArchive() {
        List<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionedRDFChainEmitter reader = new VersionedRDFChainEmitter(blobStore, TestUtilForProcessor.testListener(nodes));
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, RefNodeConstants.HAS_VERSION, SOME));

        assertThat(nodes.size(), Is.is(1));

        assertThat(nodes.get(0).getObject(),
                Is.is(RefNodeFactory.toIRI("foo:bar")));
    }

    private BlobStoreReadOnly createBlobStore() {
        return new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                if (key.equals(SOME)) {
                    return getClass().getResourceAsStream("archivetest.nq");
                } else if (key.equals(OTHER)) {
                    return getClass().getResourceAsStream("archivetest2.nq");
                } else {
                    return null;
                }
            }
        };
    }

    @Test
    public void replayArchiveMultipleVersions() {
        List<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionedRDFChainEmitter reader = new VersionedRDFChainEmitter(blobStore, TestUtilForProcessor.testListener(nodes));
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, RefNodeConstants.HAS_VERSION, RefNodeFactory.toIRI("http://some")));
        reader.on(RefNodeFactory
                .toStatement(OTHER, RefNodeConstants.HAS_PREVIOUS_VERSION, RefNodeFactory.toIRI("http://some")));

        // only version related statements are emitted
        assertThat(nodes.size(), Is.is(2));

        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("foo:bar")));
        assertThat(nodes.get(1).getObject(), Is.is(RefNodeFactory.toIRI("foo:bar")));

    }

}