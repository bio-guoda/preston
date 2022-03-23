package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public interface KeyToPath {
    URI toPath(IRI key);

    boolean supports(IRI key);
}
