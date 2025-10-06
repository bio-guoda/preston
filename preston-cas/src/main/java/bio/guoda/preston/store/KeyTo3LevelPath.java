package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyTo3LevelPath extends KeyToPathAcceptsAnyValid {

    private final URI remote;

    public KeyTo3LevelPath(URI remote) {
        this.remote = remote;
    }

    @Override
    public URI toPath(IRI key) {

        HashType type = HashKeyUtil.getHashTypeOrThrow(key);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);
        return HashKeyUtil.insertSlashIfNeeded(remote, suffix);
    }

}
