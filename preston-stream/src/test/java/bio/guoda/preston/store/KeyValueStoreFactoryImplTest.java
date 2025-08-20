package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class KeyValueStoreFactoryImplTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void getNonePutOneGetOne() throws IOException {
        File dataDir = folder.newFolder("data");
        File tmpDir = folder.newFolder("tmp");
        KeyValueStoreFactoryImpl factory = new KeyValueStoreFactoryImpl(
                new KeyValueStoreConfig(dataDir, tmpDir, 0)
        );

        KeyValueStore keyValueStore = factory.getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory());
        IRI key = RefNodeFactory.toIRI("hash://md5/5d41402abc4b2a76b9719d911017c592");
        InputStream inputStream = keyValueStore.get(key);
        assertNull(inputStream);

        BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(keyValueStore, true, HashType.md5);

        IRI otherKey = blobStore.put(IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
        assertThat(otherKey.getIRIString(), is("hash://md5/5d41402abc4b2a76b9719d911017c592"));
        String actual = IOUtils.toString(keyValueStore.get(otherKey), StandardCharsets.UTF_8);
        assertThat(actual, is("hello"));
    }

    @Ignore
    @Test
    public void getOneFromLocalZip() throws IOException, URISyntaxException {
        URL resource = getClass().getResource("data.zip");
        URI uri = resource.toURI();
        assertAvailableInRemote(URI.create("zip:" + uri.toString() + "!/data"));
    }

    @Test
    public void getOneFromLocalDataDir() throws IOException, URISyntaxException {
        URL resource = getClass().getResource("data/27/f5/27f552c25bc733d05a5cc67e9ba63850");
        URI uri = new File(resource.toURI()).getParentFile().getParentFile().getParentFile().toURI();
        assertAvailableInRemote(uri);
    }

    private void assertAvailableInRemote(URI uri) throws IOException {
        File dataDir = folder.newFolder("data");
        File tmpDir = folder.newFolder("tmp");
        IRI key = RefNodeFactory.toIRI("hash://md5/5d41402abc4b2a76b9719d911017c592");


        KeyValueStoreFactoryImpl factory = new KeyValueStoreFactoryImpl(
                new KeyValueStoreConfig(dataDir,
                        tmpDir,
                        0
                )
        );
        KeyValueStore keyValueStore = factory.getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory());
        InputStream inputStream = keyValueStore.get(key);
        assertNull(inputStream);


        KeyValueStoreFactoryImpl factoryWithRemote = new KeyValueStoreFactoryImpl(
                new KeyValueStoreConfig(dataDir,
                        tmpDir,
                        0,
                        true,
                        Arrays.asList(uri),
                        HashType.md5,
                        ContentStreamUtil.NOOP_DEREF_PROGRESS_LISTENER,
                        true
                )
        );


        KeyValueStore keyValueStoreWithRemote = factoryWithRemote
                .getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory());

        InputStream input = keyValueStoreWithRemote.get(key);
        assertNotNull(input);
        String actual = IOUtils.toString(input, StandardCharsets.UTF_8);
        assertThat(actual, is("hello"));
    }

}