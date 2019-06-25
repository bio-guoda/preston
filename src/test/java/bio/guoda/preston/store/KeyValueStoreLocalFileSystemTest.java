package bio.guoda.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;

public class KeyValueStoreLocalFileSystemTest {

    public static final String SOME_HASH = "hash://sha256/1234567890123456789012345678901234567890123456789012345678901234";
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
        filePersistence.put(SOME_HASH, "some value");

        assertThat(TestUtil.toUTF8(filePersistence.get(SOME_HASH)), is("some value"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void writeKeyTooShort() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()));
        filePersistence.get("something");
        filePersistence.put("something", "some value");

        assertThat(TestUtil.toUTF8(filePersistence.get("something")), is("some value"));

    }

    @Test
    public void writeStream() throws IOException {
        KeyValueStoreLocalFileSystem filePersistence = new KeyValueStoreLocalFileSystem(new File(path.toFile(), "tmp"), new KeyTo3LevelPath(new File(path.toFile(), "datasets").toURI()));

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
        filePersistence.put(new KeyGeneratingStream() {
            @Override
            public String generateKeyWhileStreaming(InputStream is, OutputStream os) throws IOException {
                IOUtils.copy(is, os);
                return SOME_HASH;
            }
        }, wrappedContentStream);
        assertTrue(wasClosed.get());

        assertThat(TestUtil.toUTF8(filePersistence.get(SOME_HASH)), is("some content"));


    }

}