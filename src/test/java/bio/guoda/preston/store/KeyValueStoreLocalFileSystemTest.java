package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class KeyValueStoreLocalFileSystemTest {

    public static final IRI SOME_HASH
            = RefNodeFactory.toIRI("hash://sha256/1234567890123456789012345678901234567890123456789012345678901234");

    private Path path;

    @Before
    public void init() throws IOException {
        File tempDirectory = FileUtils.getTempDirectory();
        path = Files.createTempDirectory(Paths.get(tempDirectory.toURI()), "test");
    }

    @After
    public void teardown() {
        FileUtils.deleteQuietly(path.toFile());
    }

    @Test
    public void writeAlwaysAccepting() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(
                new File(path.toFile(), "tmp"),
                new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()),
                getAlwaysAccepting()
        );

        assertNull(filePersistence.get(SOME_HASH));
        filePersistence.put(SOME_HASH, IOUtils.toInputStream("some value", StandardCharsets.UTF_8));

        assertThat(TestUtil.toUTF8(filePersistence.get(SOME_HASH)), is("some value"));

    }

    @Test
    public void writeDefault() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory());

        IRI someValueKey = RefNodeFactory.toIRI("hash://sha256/ab3d07f3169ccbd0ed6c4b45de21519f9f938c72d24124998aab949ce83bb51b");
        assertNull(filePersistence.get(someValueKey));
        filePersistence.put(someValueKey, IOUtils.toInputStream("some value", StandardCharsets.UTF_8));

        assertThat(TestUtil.toUTF8(filePersistence.get(someValueKey)), is("some value"));

    }

    public static KeyValueStreamFactory getAlwaysAccepting() {
        return (key, is) -> new ValidatingKeyValueStream() {
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

    @Test(expected = IllegalArgumentException.class)
    public void writeKeyTooShort() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory());
        IRI somethingIRI = RefNodeFactory.toIRI("something");
        filePersistence.get(somethingIRI);
    }

    @Test(expected = IllegalArgumentException.class)
    public void writeKeyTooShort2() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()), new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory());
        IRI somethingIRI = RefNodeFactory.toIRI("something");
        filePersistence.put(somethingIRI, IOUtils.toInputStream("some value", StandardCharsets.UTF_8));
    }

    @Test
    public void writeStreamWithKeyGenerator() throws IOException {

        KeyValueStreamFactory keyValueStreamFactory = (key, is) -> new ValidatingKeyValueStream() {
            @Override
            public InputStream getValueStream() {
                throw new RuntimeException("kaboom!");
            }

            @Override
            public boolean acceptValueStreamForKey(IRI key) {
                throw new RuntimeException("kaboom!");
            }
        };

        KeyValueStoreLocalFileSystem filePersistence1 =
                new KeyValueStoreLocalFileSystem(
                        new File(path.toFile(), "tmp"),
                        new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()),
                        keyValueStreamFactory);

        assertThat(filePersistence1.get(SOME_HASH), is(nullValue()));
        final InputStream someContentStream = IOUtils.toInputStream("some content", StandardCharsets.UTF_8);
        final AtomicBoolean wasClosed = new AtomicBoolean(false);
        InputStream wrappedContentStream = new InputStream() {
            @Override
            public int read() throws IOException {
                return someContentStream.read();
            }

            @Override
            public void close() throws IOException {
                wasClosed.set(true);
                someContentStream.close();
            }
        };

        assertFalse(wasClosed.get());

        filePersistence1.put((is, os) -> {
            IOUtils.copy(is, os);
            return SOME_HASH;
        }, wrappedContentStream);

        assertFalse(wasClosed.get());

        assertThat(TestUtil.toUTF8(filePersistence1.get(SOME_HASH)), is("some content"));

    }

}