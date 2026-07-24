package bio.guoda.preston.process;

import bio.guoda.preston.EnvUtil;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.ResourcesHTTP;
import bio.guoda.preston.store.DerefProgressLogger;
import bio.guoda.preston.zenodo.ZenodoContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.apache.http.HttpHeaders;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicNameValuePair;
import org.globalbioticinteractions.doi.DOI;
import org.globalbioticinteractions.doi.MalformedDOIException;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static bio.guoda.preston.ResourcesHTTP.DRYAD_AUTH_TOKEN;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertNotNull;

public class RegistryReaderDataDryadIT {

    public static final String DRYAD_CLIENT_ID = "DRYAD_CLIENT_ID";
    public static final String DRYAD_CLIENT_SECRET = "DRYAD_CLIENT_SECRET";

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
        ZenodoContext s = getOrRefreshAuthToken();
        System.setProperty(DRYAD_AUTH_TOKEN, s.getAccessToken());
        verifyAuthToken();
    }

    @Test
    public void reuseAuthToken() throws IOException {
        ZenodoContext s = getOrRefreshAuthToken(new ZenodoContext("1234"));
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
    private static ZenodoContext getOrRefreshAuthToken() throws IOException {
        return getOrRefreshAuthToken(null);
    }

    private static ZenodoContext getOrRefreshAuthToken(ZenodoContext context) throws IOException {
        if (context != null && StringUtils.isNotBlank(context.getAccessToken())) {
            return context;
        } else {
            return requestAuthToken();
        }
    }

    private static ZenodoContext requestAuthToken() throws IOException {
        Properties properties = new Properties();
        properties.load(RegistryReaderDataDryadIT.class.getResourceAsStream("dryad.properties.hidden"));
        String url = "https://datadryad.org/oauth/token";
        HttpPost post = new HttpPost(url);
        post.setHeader(HttpHeaders.CONTENT_TYPE, "application/x-www-form-urlencoded;charset=UTF-8");
        String clientId = EnvUtil.getEnvironmentVariable(DRYAD_CLIENT_ID, properties.getProperty("dryad.client.id"));
        String clientSecret = EnvUtil.getEnvironmentVariable(DRYAD_CLIENT_SECRET, properties.getProperty("dryad.client.secret"));

        if (StringUtils.isBlank(clientId)) {
            throw new IOException("to authorize with dryad, please set [" + DRYAD_CLIENT_ID + "]");
        }

        if (StringUtils.isBlank(clientSecret)) {
            throw new IOException("to authorize with dryad, please set [" + DRYAD_CLIENT_SECRET + "]");
        }

        UrlEncodedFormEntity formEntity = new UrlEncodedFormEntity(
                Arrays.asList(
                        new BasicNameValuePair("client_id", clientId),
                        new BasicNameValuePair("client_secret", clientSecret),
                        new BasicNameValuePair("grant_type", "client_credentials")
                )
        );
        post.setEntity(formEntity);

        try (InputStream inputStream = ResourcesHTTP.asInputStream(
                RefNodeFactory.toIRI(url),
                post,
                new DerefProgressLogger(),
                httpStatusCode -> false
        )) {
            JsonNode tokenConfig = new ObjectMapper().readTree(inputStream);
            assertNotNull(tokenConfig);
            return new ZenodoContext(tokenConfig.at("/access_token").asText());
        }
    }

}