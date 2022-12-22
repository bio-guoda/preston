package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyToHashURI extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;


    public KeyToHashURI(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);
        String iriString = key.getIRIString();
        return HashKeyUtil.insertSlashIfNeeded(baseURI, iriString);
    }

}
