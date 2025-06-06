package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyTo1LevelPath extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;


    public KeyTo1LevelPath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        HashType type = HashKeyUtil.getHashTypeOrThrow(key);
        int offset = type.getPrefix().length();
        String suffix = key.getIRIString().substring(offset);
        return HashKeyUtil.insertSlashIfNeeded(baseURI, suffix);
    }

}
