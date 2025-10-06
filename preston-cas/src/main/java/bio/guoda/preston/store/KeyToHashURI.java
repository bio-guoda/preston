package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyToHashURI extends KeyToPathAcceptsAnyValid {

    private final URI remote;


    public KeyToHashURI(URI remote) {
        this.remote = remote;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);
        String iriString = key.getIRIString();
        return HashKeyUtil.insertSlashIfNeeded(remote, iriString);
    }

}
