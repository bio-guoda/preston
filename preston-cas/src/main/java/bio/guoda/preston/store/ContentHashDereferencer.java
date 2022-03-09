package bio.guoda.preston.store;

import bio.guoda.preston.stream.ContentStreamFactory;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;

public class ContentHashDereferencer implements Dereferencer<InputStream> {

    private final Dereferencer<InputStream> blobStore;

    public ContentHashDereferencer(Dereferencer<InputStream> blobStore) {
        this.blobStore = blobStore;
    }

    @Override
    public InputStream get(IRI iri) throws DereferenceException {
        try {
            IRI contentHash = ContentStreamUtil.extractContentHash(iri);
            InputStream is = blobStore.get(contentHash);
            return new ContentStreamFactory(ContentStreamUtil.truncateGZNotationForVFSIfNeeded(iri)).create(is);
        } catch (IOException | IllegalArgumentException e) {
            throw new DereferenceException(iri, e);
        }
    }

}
