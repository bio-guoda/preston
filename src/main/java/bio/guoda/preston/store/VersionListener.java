package bio.guoda.preston.store;

import org.apache.commons.rdf.api.TripleLike;

import java.io.IOException;

public interface VersionListener {

    void on(TripleLike statement) throws IOException;

}
