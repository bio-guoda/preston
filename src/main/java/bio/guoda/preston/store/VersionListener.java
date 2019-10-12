package bio.guoda.preston.store;

import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

public interface VersionListener {

    void on(Triple statement) throws IOException;

}
