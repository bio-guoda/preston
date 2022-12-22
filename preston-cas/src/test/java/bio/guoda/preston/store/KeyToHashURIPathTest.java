package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class KeyToHashURIPathTest {

    @Test
    public void single() {
        KeyToPath keyTo1LevelPath = new KeyToHashURI(URI.create("https://example.com/"));
        assertThat(keyTo1LevelPath.toPath(RefNodeFactory.toIRI("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")),
                is(URI.create("https://example.com/hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")));
    }
    @Test
    public void singleNoSlash() {
        KeyToPath keyTo1LevelPath = new KeyToHashURI(URI.create("https://example.com"));
        assertThat(keyTo1LevelPath.toPath(RefNodeFactory.toIRI("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")),
                is(URI.create("https://example.com/hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")));
    }

    @Test
    public void singleMD5() {
        KeyToPath keyTo1LevelPath = new KeyToHashURI(URI.create("https://example.com"));
        assertThat(keyTo1LevelPath.toPath(RefNodeFactory.toIRI("hash://md5/b20c66c41bdacd032b4efe8d0f2c6003")),
                is(URI.create("https://example.com/hash://md5/b20c66c41bdacd032b4efe8d0f2c6003")));
    }

}