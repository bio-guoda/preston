package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;

import java.net.URI;

public class KeyTo1LevelPath implements KeyToPath {

    private final URI baseURI;

    public KeyTo1LevelPath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(String key) {
        HashKeyUtil.validateHashKey(key);
        int offset = Hasher.getHashPrefix().length();
        String suffix = key.substring(offset);
        return URI.create(baseURI.toString() + suffix);
    }

}
