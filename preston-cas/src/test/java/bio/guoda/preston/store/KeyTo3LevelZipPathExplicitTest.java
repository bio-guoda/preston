package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo3LevelZipPathExplicitTest {

    @Test
    public void attemptZipPath() {
        KeyToPath keyToLevel = new KeyTo3LevelZipPathExplicit(URI.create("https://example.org/data.zip"), HashType.sha256);
        URI uri = keyToLevel.toPath(RefNodeFactory.toIRI("hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"));
        assertThat(uri, Is.is(URI.create("zip:https://example.org/data.zip!/data/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a")));
    }

    @Test
    public void attemptZipPathMD5() {
        KeyToPath keyToLevel = new KeyTo3LevelZipPathExplicit(URI.create("https://example.org/data.zip"), HashType.md5);
        URI uri = keyToLevel.toPath(RefNodeFactory.toIRI("hash://md5/b1946ac92492d2347c6235b4d2611184"));
        assertThat(uri, Is.is(URI.create("zip:https://example.org/data.zip!/data/b1/94/b1946ac92492d2347c6235b4d2611184")));
    }

}