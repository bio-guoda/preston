package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.Dereferencer;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class DereferencerContentId implements Dereferencer<IRI> {

    private final BlobStore blobStore;

    public DereferencerContentId(BlobStore blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public IRI get(IRI uri) throws IOException {
        try (InputStream inputStream = blobStore.get(uri)) {
            IOUtils.copy(inputStream, NullOutputStream.NULL_OUTPUT_STREAM);
            return uri;
        }
    }
}
