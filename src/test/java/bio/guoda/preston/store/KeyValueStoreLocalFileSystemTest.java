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
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

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
        path = Files.createTempDirectory(Paths.get(new File("target").toURI()), "test");
    }

    @After
    public void teardown() {
        FileUtils.deleteQuietly(path.toFile());
    }

    @Test
    public void write() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()));
        assertNull(filePersistence.get(SOME_HASH));
        filePersistence.put(SOME_HASH, IOUtils.toInputStream("some value", StandardCharsets.UTF_8));

        assertThat(TestUtil.toUTF8(filePersistence.get(SOME_HASH)), is("some value"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void writeKeyTooShort() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()));
        IRI somethingIRI = RefNodeFactory.toIRI("something");
        filePersistence.get(somethingIRI);
        filePersistence.put(somethingIRI, IOUtils.toInputStream("some value", StandardCharsets.UTF_8));

        assertThat(TestUtil.toUTF8(filePersistence.get(somethingIRI)), is("some value"));

    }

    @Test
    public void writeStream() throws IOException {
        AtomicLong beforeCount = new AtomicLong(0);
        AtomicLong afterCount = new AtomicLong(0);
        List<IRI> pathRequests = new ArrayList<>();
        AtomicBoolean isAsserting = new AtomicBoolean(true);
        StringBuffer sequence = new StringBuffer();

        KeyValueStoreLocalFileSystem filePersistence =
                new KeyValueStoreLocalFileSystem(
                        new File(path.toFile(), "tmp"),
                        new KeyToPath() {
                            private KeyToPath keyToPath = new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI());
                            @Override
                            public URI toPath(IRI key) {
                                if (!isAsserting.get()) {
                                    sequence.append("toPath");
                                    pathRequests.add(key);
                                }
                                return keyToPath.toPath(key);
                            }
                        }
        ,
                        new KeyValueStoreLocalFileSystem.KeyValueStoreListener() {

                            @Override
                            public void beforePut(IRI key, long valueSizeInBytes) {
                                beforeCount.addAndGet(valueSizeInBytes);
                                sequence.append("-beforePut-");
                            }

                            @Override
                            public void afterPut(IRI key, long valueSizeInBytes) {
                                afterCount.addAndGet(valueSizeInBytes);
                                sequence.append("-afterPut");
                            }
                        });

        assertThat(filePersistence.get(SOME_HASH), is(nullValue()));
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

        isAsserting.set(false);

        filePersistence.put((is, os) -> {
            IOUtils.copy(is, os);
            return SOME_HASH;
        }, wrappedContentStream);

        isAsserting.set(true);

        assertFalse(wasClosed.get());

        assertThat(TestUtil.toUTF8(filePersistence.get(SOME_HASH)), is("some content"));

        long valueLength = (long) IOUtils.toByteArray(filePersistence.get(SOME_HASH)).length;
        assertThat(afterCount.get(), is(valueLength));
        assertThat(beforeCount.get(), is(12L));

        assertThat(pathRequests.size(), is(2));
        assertThat(pathRequests.get(0), is(SOME_HASH));
        assertThat(pathRequests.get(1), is(SOME_HASH));
        assertThat(sequence.toString(), is("toPath-beforePut-toPath-afterPut"));


    }

}