package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo1LevelSoftwareHeritageAutoDetectPathTest {

    @Test
    public void toPath() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

        URI actualPath = new KeyTo1LevelSoftwareHeritageAutoDetectPath(URI.create("https://softwareheritage.org")).toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://archive.softwareheritage.org/api/1/content/sha256:4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/raw/"));
    }

    @Test
    public void nonSoftwareHeritageNoTrailingSlash() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

        URI actualPath = new KeyTo1LevelSoftwareHeritageAutoDetectPath(URI.create("https://deeplinker.bio")).toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://deeplinker.bio/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/raw/"));
    }

    @Test
    public void nonSoftwareHeritageTrailingSlash() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

        URI actualPath = new KeyTo1LevelSoftwareHeritageAutoDetectPath(URI.create("https://deeplinker.bio/")).toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://deeplinker.bio/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/raw/"));
    }


    @Test
    public void toTryPath() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));

        URI actualPath = new KeyTo1LevelSoftwareHeritageAutoDetectPath(URI.create("https://softwareheritage.org/blabla/")).toPath(hash);
        assertThat(actualPath.toString(), Is.is("https://archive.softwareheritage.org/api/1/content/sha256:4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/raw/"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void toPathTooShort() {
        new KeyTo3LevelPath(URI.create("some://")).toPath(RefNodeFactory.toIRI("too:short"));
    }

    @Test
    public void insertSlash() {
        URI actualPath = new KeyTo1LevelSoftwareHeritageAutoDetectPath(URI.create("file:///some/sha256:"))
                .toPath(RefNodeFactory.toIRI("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
        assertThat(actualPath.toString(), Is.is("file:///some/sha256:4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/raw/"));

    }

    @Test
    public void softwareHeritage() {
        IRI key = RefNodeFactory.toIRI("hash://sha256/29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c");
        URI baseURI = URI.create("https://archive.softwareheritage.org/api/1/content/sha256:");
        URI actualPath = new KeyTo1LevelSoftwareHeritageAutoDetectPath(baseURI)
                .toPath(key);
        assertThat(actualPath.toString(), Is.is("https://archive.softwareheritage.org/api/1/content/sha256:29d30b566f924355a383b13cd48c3aa239d42cba0a55f4ccfc2930289b88b43c/raw/"));
    }
}