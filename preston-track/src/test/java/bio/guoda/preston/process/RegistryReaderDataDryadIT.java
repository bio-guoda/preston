package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.util.AuthContext;
import bio.guoda.preston.util.DryadContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.Quad;
import org.apache.http.client.methods.HttpGet;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static bio.guoda.preston.ResourcesHTTP.DRYAD_AUTH_TOKEN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

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
                is("<https://datadryad.org/api/v2/datasets/doi%3A10.5061%2Fdryad.6hdr7sr8z/versions>")
        );
    }

    @Test
    public void dryadAuthToken() throws IOException {
        AuthContext s = RegistryReaderDataDryad.getOrRefreshAuthToken(null, getProperties());
        System.setProperty(DRYAD_AUTH_TOKEN, s.getAccessToken());
        verifyAuthToken();
    }

    private static Properties getProperties() throws IOException {
        Properties properties = new Properties();
        properties.load(RegistryReaderDataDryadIT.class.getResourceAsStream("dryad.properties.hidden"));
        return properties;
    }

    @Test
    public void reuseAuthToken() throws IOException {
        AuthContext s = RegistryReaderDataDryad.getOrRefreshAuthToken(new DryadContext("1234"), getProperties());
        assertThat(s.getAccessToken(), is("1234"));
    }

    private static void verifyAuthToken() throws IOException {
        String urlString = "https://datadryad.org/api/v2/test";
        HttpGet get = new HttpGet(urlString);
        try (InputStream is = ResourcesHTTP.asInputStream(RefNodeFactory.toIRI(urlString), get, new DerefProgressLogger(), httpResultCode -> false)) {
            JsonNode msg = new ObjectMapper().readTree(is);
            assertThat(msg.at("/message").asText(), startsWith("Welcome"));
        }
    }

}