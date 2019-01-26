package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
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

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class KeyTo4LevelPathTest {

    @Test
    public void toPath() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
        String actual = new KeyTo4LevelPath().toPath(hash.getIRIString());
        assertThat(actual, is("4d/f3/c3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/data"));
    }

}