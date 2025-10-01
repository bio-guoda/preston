package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo3LevelPath extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;

    public KeyTo3LevelPath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {

        HashType type = HashKeyUtil.getHashTypeOrThrow(key);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);
        return HashKeyUtil.insertSlashIfNeeded(baseURI, suffix);
    }

}
