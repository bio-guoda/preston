package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

public class ContentQueryUtil {

    static void copyMostRecentContent(BlobStoreReadOnly blobStore,
                                      Quad quad,
                                      Persisting persisting,
                                      CopyShop copyShop) throws IOException {
        IRI version = VersionUtil.mostRecentVersion(quad);
        if (version != null) {
            copyContent(blobStore, version, persisting, copyShop);
        }
    }

    static InputStream getMostRecentContent(BlobStoreReadOnly blobStore, Quad quad, Persisting persisting) throws IOException {
        IRI version = VersionUtil.mostRecentVersion(quad);
        if (version == null) {
            throw new IOException("no content found associated to [" + quad.toString() + "]");
        }
        return getContent(blobStore, version, persisting);
    }

    public static void copyContent(
            BlobStoreReadOnly blobStore,
            IRI queryIRI,
            Persisting persisting,
            CopyShop copyShop
    ) throws IOException {
        InputStream contentStream = getContent(blobStore, queryIRI, persisting);
        copyShop.copy(contentStream, persisting.getOutputStream());
    }

    public static InputStream getContent(
            BlobStoreReadOnly blobStore,
            IRI queryIRI,
            Persisting persisting) throws IOException {
        InputStream contentStream;

        BlobStoreReadOnly store = Persisting.resolvingBlobStore(blobStore, persisting);

        try {
            contentStream = store.get(
                    new IRIFixingProcessor()
                            .process(queryIRI)
            );
            if (contentStream == null) {
                throw new IOException("[" + queryIRI.getIRIString() + "] not found.");
            }
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryIRI.getIRIString() + "]", e);
        }
        return contentStream;
    }
}
