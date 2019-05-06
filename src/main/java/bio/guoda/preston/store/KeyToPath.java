package bio.guoda.preston.store;

import java.net.URI;

public interface KeyToPath {
    URI toPath(String key);
}
