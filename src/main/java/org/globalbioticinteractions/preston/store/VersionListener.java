package org.globalbioticinteractions.preston.store;

import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

public interface VersionListener {

    void onVersion(Triple statement) throws IOException;

}
