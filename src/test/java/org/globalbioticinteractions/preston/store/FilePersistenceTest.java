package org.globalbioticinteractions.preston.store;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.globalbioticinteractions.preston.Hasher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.*;

public class FilePersistenceTest {

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
    public void toPath() {
        URI hash = Hasher.calcSHA256("bla");
        assertThat(hash.toString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
        String actual = FilePersistence.toPath(hash.toString());
        assertThat(actual, is("4d/f3/c3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
    }

    @Test
    public void write() throws IOException {
        FilePersistence filePersistence = new FilePersistence(new File(path.toFile(), "tmp"), new File(path.toFile(), "datasets"));
        filePersistence.get("somethinggggggggggggggggggggg");
        filePersistence.put("somethinggggggggggggggggggggg", "some value");

        assertThat(TestUtil.toUTF8(filePersistence.get("somethinggggggggggggggggggggg")), is("some value"));

    }

    @Test(expected = IllegalArgumentException.class)
    public void writeKeyTooShort() throws IOException {
        FilePersistence filePersistence = new FilePersistence(new File(path.toFile(), "tmp"), new File(path.toFile(), "datasets"));
        filePersistence.get("something");
        filePersistence.put("something", "some value");

        assertThat(TestUtil.toUTF8(filePersistence.get("something")), is("some value"));

    }

    @Test
    public void writeStream() throws IOException {
        FilePersistence filePersistence = new FilePersistence(new File(path.toFile(), "tmp"), new File(path.toFile(), "datasets"));

        assertThat(filePersistence.get("some keyyyyyyyyyyyyyyyyyy"), is(nullValue()));
        filePersistence.put(new KeyGeneratingStream() {
            @Override
            public String generateKeyWhileStreaming(InputStream is, OutputStream os) throws IOException {
                IOUtils.copy(is, os);
                return "some keyyyyyyyyyyyyyyyyyy";
            }
        }, IOUtils.toInputStream("some content", StandardCharsets.UTF_8));

        assertThat(TestUtil.toUTF8(filePersistence.get("some keyyyyyyyyyyyyyyyyyy")), is("some content"));

    }

}