package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class DereferencerContentAddressed implements Dereferencer<IRI> {
    private final Dereferencer<InputStream> dereferencer;
    private final BlobStore blobStore;

    public DereferencerContentAddressed(Dereferencer<InputStream> dereferencer, BlobStore blobStore) {
        this.dereferencer = dereferencer;
        this.blobStore = blobStore;
    }

    @Override
    public IRI dereference(IRI uri) throws IOException {
        try (InputStream data = dereferencer == null ? null : dereferencer.dereference(uri)) {
            return data == null ? null : blobStore.putBlob(data);
        }
    }

}
