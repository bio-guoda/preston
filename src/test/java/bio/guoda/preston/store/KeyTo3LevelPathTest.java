package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KeyTo3LevelPathTest {

    @Test
    public void toPath() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

        String actualPath = new KeyTo3LevelPath().toPath(hash.getIRIString());
        assertThat(actualPath, Is.is("4d/f3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void toPathTooShort() {
        new KeyTo3LevelPath().toPath("too:short");
    }

}