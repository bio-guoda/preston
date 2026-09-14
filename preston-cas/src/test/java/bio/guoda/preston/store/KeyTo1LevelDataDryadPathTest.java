package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class KeyTo1LevelDataDryadPathTest {

    public static final IRI HASH_IRI = RefNodeFactory.toIRI("hash://sha256/de75afb0a7222a5591e3e39869befa2c0fb5d7a2e4ade47d49380e1ccb8b39fb");

    @Test
    public void extractDownloadUri() throws IOException {
        InputStream resourceAsStream = getTestResponse();
        String downloadUri = KeyTo1LevelDataDryadPath.extractDownloadLink(resourceAsStream);
        assertThat(downloadUri, is("/api/v2/files/3985010/download"));
    }

    private InputStream getTestResponse() {
        return getClass().getResourceAsStream("datadryad-response-sha256.json");
    }


    @Test
    public void createRequestUrl() {
        String requestSuffix = KeyTo1LevelDataDryadPath.getRequestSuffix(HASH_IRI);

        assertThat(requestSuffix,
                is("/files/digest/de75afb0a7222a5591e3e39869befa2c0fb5d7a2e4ade47d49380e1ccb8b39fb")
        );
    }

    @Test
    public void toPath() throws URISyntaxException {
        KeyTo1LevelDataDryadPath keyTo1LevelDataDryadPath = new KeyTo1LevelDataDryadPath(new URI("https://datadryad.org"), new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                assertThat(uri.getIRIString(), is("https://datadryad.org/api/v2/files/digest/de75afb0a7222a5591e3e39869befa2c0fb5d7a2e4ade47d49380e1ccb8b39fb"));
                return getTestResponse();
            }
        });
        URI path = keyTo1LevelDataDryadPath.toPath(HASH_IRI);

        assertThat(path, is(notNullValue()));
        assertThat(path.toString(), is("https://datadryad.org/api/v2/files/3985010/download"));
    }

    @Test
    public void toPathWithKaboom() throws URISyntaxException {
        KeyTo1LevelDataDryadPath keyTo1LevelDataDryadPath = new KeyTo1LevelDataDryadPath(new URI("https://datadryad.org"), new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        });
        URI path = keyTo1LevelDataDryadPath.toPath(HASH_IRI);

        assertThat(path, is(nullValue()));
    }

    @Test
    public void toPathNoMatches() throws URISyntaxException {
        KeyTo1LevelDataDryadPath keyTo1LevelDataDryadPath = new KeyTo1LevelDataDryadPath(new URI("https://datadryad.org"), new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("datadryad-response-sha256-empty.json");
            }
        });
        URI path = keyTo1LevelDataDryadPath.toPath(HASH_IRI);

        assertThat(path, is(nullValue()));
    }

    @Test
    public void toPathMalformed() throws URISyntaxException {
        KeyTo1LevelDataDryadPath keyTo1LevelDataDryadPath = new KeyTo1LevelDataDryadPath(new URI("https://datadryad.org"), new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("datadryad-response-sha256-malformed.json");
            }
        });
        URI path = keyTo1LevelDataDryadPath.toPath(HASH_IRI);

        assertThat(path, is(nullValue()));
    }

}