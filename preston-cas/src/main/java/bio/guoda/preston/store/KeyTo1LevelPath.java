package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyTo1LevelPath implements KeyToPath {

    private final URI baseURI;

    public KeyTo1LevelPath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);
        int offset = HashType.sha256.getPrefix().length();
        String suffix = key.getIRIString().substring(offset);
        return HashKeyUtil.insertSlashIfNeeded(baseURI, suffix);
    }

}
