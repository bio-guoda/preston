package bio.guoda.preston.store;

import org.apache.commons.rdf.api.Quad;

import java.io.IOException;

public interface VersionListener {

    void on(Quad statement) throws IOException;

}
