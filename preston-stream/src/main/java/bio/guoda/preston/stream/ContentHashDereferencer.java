package bio.guoda.preston.stream;

import bio.guoda.preston.store.DereferenceException;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
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
            IRI contentHash = HashKeyUtil.extractContentHash(iri);
            InputStream is = blobStore.get(contentHash);
            return new ContentStreamFactory(ContentStreamUtil.truncateGZNotationForVFSIfNeeded(iri)).create(is);
        } catch (IOException | IllegalArgumentException e) {
            throw new DereferenceException(iri, e);
        }
    }

}
