package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public abstract class KeyToPathAcceptsAnyValid implements KeyToPath {
    @Override
    public boolean supports(IRI key) {
        return HashKeyUtil.isValidHashKey(key);
    }
}
