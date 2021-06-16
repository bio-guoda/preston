package bio.guoda.preston.cmd;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;

public class VersionRetrieverTest {

    @Test(expected = IllegalArgumentException.class)
    public void retrieveSingleVersionFail() {
        new VersionRetriever(key -> {
            throw new IllegalArgumentException();
        }).on(RefNodeFactory.toStatement(BIODIVERSITY_DATASET_GRAPH, HAS_VERSION, RefNodeFactory.toIRI("some")));
    }

    @Test
    public void retrieveSingleVersionSuccess() {
        final List<IRI> requested = new ArrayList<IRI>();
        IRI some = RefNodeFactory.toIRI("some");
        new VersionRetriever(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                requested.add(key);
                return IOUtils.toInputStream("hello!", StandardCharsets.UTF_8);
            }
        }).on(RefNodeFactory.toStatement(BIODIVERSITY_DATASET_GRAPH, HAS_VERSION, some));

        assertThat(requested.size(), Is.is(1));
        assertThat(requested.get(0), Is.is(some));
    }

    @Test
    public void retrieveTwoVersionsSuccess() {
        final List<IRI> requested = new ArrayList<IRI>();
        IRI some = RefNodeFactory.toIRI("some");
        IRI other = RefNodeFactory.toIRI("other");
        VersionRetriever retriever = new VersionRetriever(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                requested.add(key);
                return IOUtils.toInputStream("hello!", StandardCharsets.UTF_8);
            }
        });
        retriever.on(RefNodeFactory.toStatement(BIODIVERSITY_DATASET_GRAPH, HAS_VERSION, some));
        retriever.on(RefNodeFactory.toStatement(other, HAS_PREVIOUS_VERSION, some));

        assertThat(requested.size(), Is.is(2));
        assertThat(requested.get(0), Is.is(some));
        assertThat(requested.get(1), Is.is(other));
    }

}