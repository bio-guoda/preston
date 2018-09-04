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

    @Test
    public void replayArchive() {
        List<Triple> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("archivetest.nq");
            }
        };
        VersionLogger reader = new VersionLogger(blobStore, nodes::add);
        reader.on(RefNodeFactory
                .toStatement(RefNodeConstants.ARCHIVE, RefNodeConstants.HAS_VERSION, RefNodeFactory.toIRI("http://some")));

        assertThat(nodes.size(), Is.is(10));


        assertThat(nodes.get(0).getObject(), Is.is(RefNodeFactory.toIRI("http://www.w3.org/ns/prov#SoftwareAgent")));
    }

}