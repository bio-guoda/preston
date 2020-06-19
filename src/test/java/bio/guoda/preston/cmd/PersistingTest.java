package bio.guoda.preston.cmd;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystemTest;
import org.apache.commons.io.IOUtils;
import org.hamcrest.core.StringStartsWith;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

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
        persisting.setRemoteURIs(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(KeyValueStoreLocalFileSystemTest.getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        String evolutionOfMan = IOUtils.toString(inputStream, StandardCharsets.UTF_8);
        assertThat(evolutionOfMan, startsWith("THE EVOLUTION OF MAN"));
    }

    @Test
    public void localFilePathInFolders() throws URISyntaxException, IOException {
        Persisting persisting = new Persisting();
        URL resource = getClass().getResource("/bio/guoda/preston/data/a1/2d/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da");
        assertThat(resource, is(not(nullValue())));
        URI baseURI = new File(resource.toURI()).getParentFile().getParentFile().getParentFile().toURI();
        persisting.setRemoteURIs(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(KeyValueStoreLocalFileSystemTest.getAlwaysAccepting());
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
        persisting.setRemoteURIs(Collections.singletonList(baseURI));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(KeyValueStoreLocalFileSystemTest.getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12226335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        assertNull(inputStream);
    }

    @Test
    public void githubPlainFolder() throws IOException {

        Persisting persisting = new Persisting();
        persisting.setNoLocalCache(true);
        persisting.setRemoteURIs(Collections.singletonList(URI.create("https://raw.githubusercontent.com/bio-guoda/preston-amazon/master/data/")));

        KeyValueStore keyValueStore = persisting.getKeyValueStore(KeyValueStoreLocalFileSystemTest.getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/052c70c60e7c17123a1bdf18d2ce607cee1b0d96157b6174aae08a2f2f8d53d8"));
        assertNotNull(inputStream);

        assertThat(org.apache.cxf.helpers.IOUtils.toString(inputStream, StandardCharsets.UTF_8.name()), StringStartsWith.startsWith("{\"key\":\"58414378-4fb2-47e0-8dd5-8b55d5c77117\""));

    }

    @Test
    public void gitHubTarGz() throws IOException {

        Persisting persisting = new Persisting();
        persisting.setRemoteURIs(Collections.singletonList(URI.create("https://raw.githubusercontent.com/bio-guoda/preston/346c2f16bdeff39b385ed86717015bf69f0301d4/src/test/resources/")));
        persisting.setNoLocalCache(true);

        KeyValueStore keyValueStore = persisting.getKeyValueStore(KeyValueStoreLocalFileSystemTest.getAlwaysAccepting());
        InputStream inputStream = keyValueStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        assertNotNull(inputStream);

        assertThat(org.apache.cxf.helpers.IOUtils.toString(inputStream, StandardCharsets.UTF_8.name()),
                StringStartsWith.startsWith("THE EVOLUTION OF MAN"));

    }


}