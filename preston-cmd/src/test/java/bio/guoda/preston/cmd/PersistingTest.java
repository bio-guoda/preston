package bio.guoda.preston.cmd;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreUtil;
import bio.guoda.preston.store.ValidatingKeyValueStreamFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamWithViolations;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.StringStartsWith;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class PersistingTest {

    @Test
    public void localFileInTarGz() throws URISyntaxException, IOException {
        Persisting persisting = new Persisting();
        URL resource = getClass().getResource("/preston-a1.tar.gz");
        assertThat(resource, is(not(nullValue())));
        URI baseURI = new File(resource.toURI()).getParentFile().toURI();
        persisting.setRemotes(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        String evolutionOfMan = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        assertThat(evolutionOfMan, startsWith("THE EVOLUTION OF MAN"));
    }

    @Test
    public void localFileInTarGzShort() throws URISyntaxException, IOException {
        Persisting persisting = new Persisting();
        URL resource = getClass().getResource("/preston-a.tar.gz");
        assertThat(resource, is(not(nullValue())));
        URI baseURI = new File(resource.toURI()).getParentFile().toURI();
        persisting.setRemotes(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        String evolutionOfMan = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        assertThat(evolutionOfMan, startsWith("THE EVOLUTION OF MAN"));
    }

    @Test
    public void defaultRemotes() {
        Persisting persisting = new Persisting();
        List<URI> remotes = persisting.getRemotes();
        assertThat(remotes.size(), is(0));
    }

    @Test
    public void localFilePathInFolders() throws URISyntaxException, IOException {
        Persisting persisting = new Persisting();
        URL resource = getClass().getResource("/bio/guoda/preston/data/a1/2d/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da");
        assertThat(resource, is(not(nullValue())));
        URI baseURI = new File(resource.toURI()).getParentFile().getParentFile().getParentFile().toURI();
        persisting.setRemotes(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        String evolutionOfMan = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        assertThat(evolutionOfMan, startsWith("THE EVOLUTION OF MAN"));
    }

    @Test
    public void localFileNonExisting() throws URISyntaxException, IOException {
        Persisting persisting = new Persisting();
        URL resource = getClass().getResource("/bio/guoda/preston/data/a1/2d/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da");
        assertThat(resource, is(not(nullValue())));
        URI baseURI = new File(resource.toURI()).getParentFile().getParentFile().getParentFile().toURI();
        persisting.setRemotes(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12226335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        assertNull(inputStream);
    }

    @Test
    public void githubPlainFolder() throws IOException {
        Persisting persisting = new Persisting();
        persisting.setCacheEnabled(false);
        persisting.setRemotes(Collections.singletonList(URI.create("https://raw.githubusercontent.com/bio-guoda/preston-amazon/master/data/")));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/052c70c60e7c17123a1bdf18d2ce607cee1b0d96157b6174aae08a2f2f8d53d8"));
        assertNotNull(inputStream);

        assertThat(org.apache.commons.io.IOUtils.toString(inputStream, StandardCharsets.UTF_8), StringStartsWith.startsWith("{\"key\":\"58414378-4fb2-47e0-8dd5-8b55d5c77117\""));

    }

    @Test
    public void gitHubTarGz() throws IOException {

        Persisting persisting = new Persisting();
        persisting.setRemotes(Collections.singletonList(URI.create("https://raw.githubusercontent.com/bio-guoda/preston/346c2f16bdeff39b385ed86717015bf69f0301d4/src/test/resources/")));
        persisting.setCacheEnabled(false);

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        assertNotNull(inputStream);

        assertThat(org.apache.commons.io.IOUtils.toString(inputStream, StandardCharsets.UTF_8),
                StringStartsWith.startsWith("THE EVOLUTION OF MAN"));

    }

    @Test
    public void softwareHeritageDetect() throws IOException {

        Persisting persisting = new Persisting();
        persisting.setRemotes(Collections.singletonList(URI.create("https://softwareheritage.org")));
        persisting.setCacheEnabled(false);

        KeyValueStore keyValueStore = persisting.getKeyValueStore(getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/162a17cbd1da43ac4eaba36b936104b09967ac29bfda7294201df34659e1a656"));
        assertNotNull(inputStream);

        assertThat(org.apache.commons.io.IOUtils.toString(inputStream, StandardCharsets.UTF_8),
                StringStartsWith.startsWith("{\"key\":\"e10cb8d7-cf2d-4b2f-9758-76dbead48965\""));

    }

    @Test
    public void fileDereferencer() throws IOException, URISyntaxException {

        Dereferencer<InputStream> deref = KeyValueStoreUtil.getInputStreamDereferencerFile(new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        });

        URL resource = getClass().getResource("test.txt");
        InputStream inputStream = deref.get(RefNodeFactory.toIRI(resource.toURI()));

        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), is("this is a test"));

    }

    @Test
    public void fileDereferencerNonFileURL() throws IOException, URISyntaxException {

        Dereferencer<InputStream> deref = KeyValueStoreUtil.getInputStreamDereferencerFile(new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        });

        InputStream inputStream = deref.get(RefNodeFactory.toIRI("https://example.org"));
        assertNull(inputStream);
    }

    public static ValidatingKeyValueStreamFactory getAlwaysAccepting() {
        return (key, is) -> new ValidatingKeyValueStreamWithViolations() {
            @Override
            public InputStream getValueStream() {
                return is;
            }

            @Override
            public boolean acceptValueStreamForKey(IRI key) {
                return true;
            }
        };
    }


}