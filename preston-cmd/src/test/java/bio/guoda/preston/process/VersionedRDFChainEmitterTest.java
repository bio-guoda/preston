package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtilForProcessor;
import bio.guoda.preston.store.VersionedRDFChainEmitter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

public class VersionedRDFChainEmitterTest {

    private static final IRI SOME = RefNodeFactory.toIRI("hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03");
    private static final IRI OTHER = RefNodeFactory.toIRI("hash://sha256/e258d248fda94c63753607f7c4494ee0fcbe92f1a76bfdac795c9d84101eb317");

    @Test
    public void replayArchive() {
        List<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionedRDFChainEmitter reader = new VersionedRDFChainEmitter(blobStore, getEmitterFactory(), TestUtilForProcessor.testListener(nodes));
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, RefNodeConstants.HAS_VERSION, SOME));

        assertThat(nodes.size(), Is.is(1));

        assertThat(nodes.get(0).getObject(),
                Is.is(RefNodeFactory.toIRI("hash://sha1/9591818c07e900db7e1e0bc4b884c945e6a61b24")));
    }

    public EmittingStreamFactory getEmitterFactory() {
        return new EmittingStreamFactory() {
            @Override
            public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                return new EmittingStreamOfAnyVersions(emitter, context);
            }
        };
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
        VersionedRDFChainEmitter reader = new VersionedRDFChainEmitter(blobStore, getEmitterFactory(), TestUtilForProcessor.testListener(nodes));
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, RefNodeConstants.HAS_VERSION, SOME));
        reader.on(RefNodeFactory
                .toStatement(OTHER, RefNodeConstants.HAS_PREVIOUS_VERSION, SOME));

        // only version related statements are emitted
        assertThat(nodes.size(), Is.is(2));

        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha1/9591818c07e900db7e1e0bc4b884c945e6a61b24")));
        assertThat(nodes.get(1).getObject(), Is.is(RefNodeFactory.toIRI("hash://sha1/f572d396fae9206628714fb2ce00f72e94f2258f")));

    }

}