package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo5LevelPathTest {

    @Test
    public void toPath() {
        IRI hash = Hasher.calcHashIRI("bla", HashType.sha256);
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
        URI actual = new KeyTo5LevelPath(URI.create("some:///")).toPath(hash);
        assertThat(actual, is(URI.create("some:///4d/f3/c3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/data")));
    }

    @Test
    public void toPathMD5() {
        IRI hash = Hasher.calcHashIRI("bla", HashType.md5);
        assertThat(hash.getIRIString(), is("hash://md5/128ecf542a35ac5270a87dc740918404"));
        URI actual = new KeyTo5LevelPath(URI.create("some:///")).toPath(hash);
        assertThat(actual, is(URI.create("some:///12/8e/cf/128ecf542a35ac5270a87dc740918404/data")));
    }

    @Test
    public void generatePathFromUUID() {
        assertThat(new KeyTo5LevelPath(URI.create("some:///")).toPath(RefNodeFactory.toIRI("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb")),
                Is.is(URI.create("some:///3f/c9/b6/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb/data")));
    }


}