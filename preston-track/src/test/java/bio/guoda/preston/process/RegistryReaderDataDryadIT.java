package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.junit.Test;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class RegistryReaderDataDryadIT {

    @Test
    public void doiToDataDryad() throws MalformedDOIException, URISyntaxException {

        List<Quad> statements = new ArrayList<>();

        StatementEmitter emitter = new StatementEmitter() {

            @Override
            public void emit(Quad statement) {
                statements.add(statement);
            }
        };

        DOI doi = DOI.create("10.5061/dryad.6hdr7sr8z");
        RegistryReaderDataDryad.emitDataDryadEndpoint(doi, emitter);

        assertThat(statements.size(), is(1));
        Quad receivedStatement = statements.get(0);
        assertThat(receivedStatement.getSubject().ntriplesString(),
                is("<https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.6hdr7sr8z>")
        );
    }

}