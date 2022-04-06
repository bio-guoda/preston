package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

public class ProvenanceTrackerImpl implements ProvenanceTracker {

    private final HexaStoreReadOnly hexastore;

    public ProvenanceTrackerImpl(HexaStoreReadOnly hexastore) {
        this.hexastore = hexastore;
    }

    @Override
    public void findDescendants(IRI provenanceAnchor, VersionListener listener) throws IOException {
        VersionUtil.findMostRecentVersion(provenanceAnchor, hexastore, listener);
    }

    @Override
    public void findOrigins(IRI provenanceAnchor, VersionListener listener) throws IOException {
    }
}
