package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.hamcrest.MatcherAssert.assertThat;

public class KeyTo3LevelTarGzPathShorterTest {

    @Test
    public void attemptTgzPath() {
        KeyToPath keyToLevel = new KeyTo3LevelTarGzPathShorter(URI.create("https://example.org/"), HashType.sha256);
        URI uri = keyToLevel.toPath(RefNodeFactory.toIRI("hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"));
        assertThat(uri, Is.is(URI.create("tgz:https://example.org/preston.tar.gz!/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a")));
    }

}