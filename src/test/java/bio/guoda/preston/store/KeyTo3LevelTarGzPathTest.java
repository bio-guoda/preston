package bio.guoda.preston.store;

import org.hamcrest.core.Is;
import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertThat;

public class KeyTo3LevelTarGzPathTest {

    @Test
    public void attemptTgzPath() {
        KeyToPath keyToLevel = new KeyTo3LevelTarGzPath(URI.create("https://example.org/"));
        URI uri = keyToLevel.toPath("hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        assertThat(uri, Is.is(URI.create("tgz:https://example.org/preston-2a.tar.gz!/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a")));
    }

}