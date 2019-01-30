package bio.guoda.preston.process;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertThat;

public class VersionLoggerTest {

    public static final IRI SOME = RefNodeFactory.toIRI("http://some");
    public static final IRI OTHER = RefNodeFactory.toIRI("http://other");
    @Test
    public void replayArchive() {
        List<Triple> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionLogger reader = new VersionLogger(blobStore, nodes::add);
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.ARCHIVE, RefNodeConstants.HAS_VERSION, SOME));

        assertThat(nodes.size(), Is.is(10));

        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
        assertThat(nodes.get(3).getSubject(), Is.is(RefNodeFactory.toIRI("5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1")));
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
        List<Triple> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = createBlobStore();
        VersionLogger reader = new VersionLogger(blobStore, nodes::add);
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.ARCHIVE, RefNodeConstants.HAS_VERSION, RefNodeFactory.toIRI("http://some")));
        reader.on(RefNodeFactory
                .toStatement(OTHER, RefNodeConstants.HAS_PREVIOUS_VERSION, RefNodeFactory.toIRI("http://some")));

        assertThat(nodes.size(), Is.is(20));

        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
        assertThat(nodes.get(3).getSubject(), Is.is(RefNodeFactory.toIRI("5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1")));
        assertThat(nodes.get(10).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
        assertThat(nodes.get(13).getSubject(), Is.is(RefNodeFactory.toIRI("653d230a-4cdc-46da-99b6-3df33fb48a55")));

    }

}