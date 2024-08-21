package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.process.StopProcessingException;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.Dereferencer;
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

    public static void copyContent(
            Dereferencer<InputStream> blobStore,
            IRI queryIRI,
            Persisting persisting,
            CopyShop copyShop
    ) throws IOException {
        try {
            InputStream contentStream = getContent(blobStore, queryIRI, persisting);
            copyShop.copy(contentStream, persisting.getOutputStream());
        } catch (StopProcessingException ex) {
            if (persisting.shouldKeepProcessing()) {
                throw ex;
            }
        }
    }

    public static InputStream getContent(
            Dereferencer<InputStream> blobStore,
            IRI queryIRI,
            Persisting persisting) throws IOException {

        DerferencerFactory factory = new DerferencerFactory() {

            @Override
            public BlobStoreReadOnly create() {
                return Persisting.resolvingBlobStore(blobStore, persisting);
            }
        };

        return getContent(queryIRI, factory);
    }

    public static InputStream getContent(IRI queryIRI, DerferencerFactory factory) throws IOException {
        InputStream contentStream;

        Dereferencer<InputStream> store = factory.create();

        try {
            contentStream = store.get(
                    new IRIFixingProcessor()
                            .process(queryIRI)
            );
            if (contentStream == null) {
                throw new IOException("[" + queryIRI.getIRIString() + "] not found.");
            }
        } catch (IOException | IllegalArgumentException e) {
            throw new IOException("problem retrieving [" + queryIRI.getIRIString() + "]", e);
        }
        return contentStream;
    }
}
