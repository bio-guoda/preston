package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class DereferencerContentAddressed implements Dereferencer3<IRI> {
    private final Dereferencer3<InputStream> dereferencer;
    private final BlobStore blobStore;

    public DereferencerContentAddressed(Dereferencer3<InputStream> dereferencer, BlobStore blobStore) {
        this.dereferencer = dereferencer;
        this.blobStore = blobStore;
    }

    @Override
    public IRI dereference(IRI uri) throws IOException {
        InputStream data = dereferencer.dereference(uri);
        return data == null ? null : blobStore.putBlob(data);
    }

}
