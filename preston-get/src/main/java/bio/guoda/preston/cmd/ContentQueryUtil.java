package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

public class ContentQueryUtil {
    static void handleContentQuery(BlobStoreReadOnly blobStore, Quad quad, Persisting persisting) throws IOException {
        IRI version = VersionUtil.mostRecentVersionForStatement(quad);
        if (version != null) {
            handleContentQuery(blobStore, version, persisting);
        }
    }

    public static void handleContentQuery(BlobStoreReadOnly blobStore, IRI queryIRI, Persisting persisting) throws IOException {

        BlobStoreReadOnly query = Persisting.resolvingBlobStore(blobStore, persisting);

        try {
            InputStream contentStream = query.get(
                    new IRIFixingProcessor()
                            .process(queryIRI)
            );
            if (contentStream == null) {
                throw new IOException("[" + queryIRI.getIRIString() + "] not found.");
            }
            IOUtils.copyLarge(contentStream, persisting.getOutputStream());
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryIRI.getIRIString() + "]", e);
        }
    }
}
