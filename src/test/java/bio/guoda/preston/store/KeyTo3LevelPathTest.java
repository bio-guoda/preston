package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class KeyTo3LevelPathTest {

    @Test
    public void toPath() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

        URI actualPath = new KeyTo3LevelPath(URI.create("file:///")).toPath(hash);
        assertThat(actualPath.toString(), Is.is("file:///4d/f3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void toPathTooShort() {
        new KeyTo3LevelPath(URI.create("some://")).toPath(RefNodeFactory.toIRI("too:short"));
    }

    @Test
    public void insertSlash() {
        URI actualPath = new KeyTo3LevelPath(URI.create("file:///some/path")).toPath(RefNodeFactory.toIRI("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
        assertThat(actualPath.toString(), Is.is("file:///some/path/4d/f3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

    }

    @Test
    public void internetArchive() {
        IRI key = RefNodeFactory.toIRI("hash://sha256/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c");
        URI baseURI = URI.create("https://archive.org/download/biodiversity-dataset-archives/data.zip/data");
        URI actualPath = new KeyTo3LevelPath(baseURI)
                .toPath(key);
        assertThat(actualPath.toString(), Is.is("https://archive.org/download/biodiversity-dataset-archives/data.zip/data/29/d3/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c"));
    }

}