package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.core.Is.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo1LevelPathTest {

    @Test
    public void single() {
        KeyTo1LevelPath keyTo1LevelPath = new KeyTo1LevelPath(URI.create("https://example.com/"));
        assertThat(keyTo1LevelPath.toPath(RefNodeFactory.toIRI("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")),
                is(URI.create("https://example.com/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")));
    }

    @Test
    public void singleMD5() {
        KeyTo1LevelPath keyTo1LevelPath = new KeyTo1LevelPath(URI.create("https://example.com/"));
        assertThat(keyTo1LevelPath.toPath(RefNodeFactory.toIRI("hash://md5/3cd7a0db76ff9dca48979e24c39b408c")),
                is(URI.create("https://example.com/3cd7a0db76ff9dca48979e24c39b408c")));
    }

    @Test
    public void singleNoSlash() {
        KeyTo1LevelPath keyTo1LevelPath = new KeyTo1LevelPath(URI.create("https://example.com"));
        assertThat(keyTo1LevelPath.toPath(RefNodeFactory.toIRI("hash://sha256/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")),
                is(URI.create("https://example.com/4df3c3f68fcc83b27e9d42c90431a72499f17875c81a599b566c9889b9696703")));
    }

}