package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyToHashURI extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;
    private HashType type;


    public KeyToHashURI(URI baseURI, HashType type) {
        this.baseURI = baseURI;
        this.type = type;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);
        String iriString = key.getIRIString();
        return HashKeyUtil.insertSlashIfNeeded(baseURI, iriString);
    }

}
