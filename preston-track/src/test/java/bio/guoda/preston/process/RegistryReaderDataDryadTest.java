package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertTrue;

public class RegistryReaderDataDryadTest {

    @Test
    public void fromVersionsToFiles() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderDataDryad registryReader = new RegistryReaderDataDryad(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("dryad-versions.json");
            }
        }, new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        registryReader.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI("https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.6hdr7sr8z/versions"),
                HAS_VERSION,
                RefNodeFactory.toIRI("foo:bar"))
        );

        assertThat(statements.size(), is(3));
        assertThat(statements.get(0).getSubject().ntriplesString(), is("<foo:bar>"));
        Quad lastStatement = statements.get(statements.size() - 1);
        assertThat(lastStatement.getSubject().ntriplesString(), is("<https://datadryad.org//api/v2/versions/355108/files>"));
        assertThat(lastStatement.getPredicate().ntriplesString(), is("<http://purl.org/pav/hasVersion>"));
        assertThat(lastStatement.getObject().ntriplesString(), startsWith("_:"));
    }

    @Test
    public void fileToEndpoints() {
        List<Quad> statements = new ArrayList<>();
        RegistryReaderDataDryad registryReader = new RegistryReaderDataDryad(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("dryad-files.json");
            }
        }, new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        });

        String filesEndpoint = "https://datadryad.org/api/v2/versions/355108/files";
        registryReader.on(RefNodeFactory.toStatement(
                RefNodeFactory.toIRI(filesEndpoint),
                HAS_VERSION,
                RefNodeFactory.toIRI("foo:bar"))
        );

        assertTrue(RegistryReaderDataDryad.isFilesEndpoint(RefNodeFactory.toIRI(filesEndpoint)));

        assertThat(statements.size(), is(9));
        assertThat(statements.get(0).getSubject().ntriplesString(), is("<https://datadryad.org/api/v2/versions/355108//api/v2/files/3985003/download>"));
        Quad lastStatement = statements.get(statements.size() - 1);
        assertThat(lastStatement.getSubject().ntriplesString(), is("<https://datadryad.org/api/v2/versions/355108//api/v2/files/3985010/download>"));
        assertThat(lastStatement.getPredicate().ntriplesString(), is("<http://purl.org/pav/hasVersion>"));
        assertThat(lastStatement.getObject().ntriplesString(), startsWith("_:"));
    }

}