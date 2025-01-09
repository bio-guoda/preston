package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyTo1LevelPath;
import bio.guoda.preston.store.KeyTo3LevelPath;
import bio.guoda.preston.store.KeyToPath;

import java.net.URI;

public class KeyToPathFactoryDepth implements KeyToPathFactory {

    private final URI baseURI;
    private final int depth;
    private KeyToPath keyToPathLocal;

    public KeyToPathFactoryDepth(URI baseURI, int depth) {
        if (depth != 0 && depth != 2) {
            throw new IllegalArgumentException("only directory depths in {0,2} are supported, but found [" + depth + "]");
        }

        this.baseURI = baseURI;
        this.depth = depth;
        this.keyToPathLocal = null;
    }

    @Override
    public KeyToPath getKeyToPath() {
        if (this.keyToPathLocal == null) {
            if (this.depth == 2) {
                this.keyToPathLocal = new KeyTo3LevelPath(baseURI);
            } else if (this.depth == 0) {
                this.keyToPathLocal = new KeyTo1LevelPath(baseURI);
            }
        }
        return this.keyToPathLocal;
    }
}
