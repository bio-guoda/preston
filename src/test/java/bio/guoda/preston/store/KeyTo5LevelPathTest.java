package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class KeyTo5LevelPathTest {

    @Test
    public void toPath() {
        IRI hash = Hasher.calcSHA256("bla");
        assertThat(hash.getIRIString(), is("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703"));
        String actual = new KeyTo5LevelPath().toPath(hash.getIRIString());
        assertThat(actual, is("4d/f3/c3/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703/data"));
    }

    @Test
    public void generatePathFromUUID() {
        assertThat(new KeyTo5LevelPath().toPath("hash://sha256/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb"),
                Is.is("3f/c9/b6/3fc9b689459d738f8c88a3a48aa9e33542016b7a4052e001aaa536fca74813cb/data"));
    }


}