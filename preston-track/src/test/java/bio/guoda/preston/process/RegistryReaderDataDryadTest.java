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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

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

        assertThat(statements.size(), is(greaterThan(1)));
    }

}