package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.process.StopProcessingException;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.VersionUtil;
import bio.guoda.preston.stream.ContentStreamException;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;

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
                return BlobStoreUtil.createResolvingBlobStoreFor(blobStore, persisting);
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

    public static InputStream getContent(IRI contentId, DerferencerFactory derferencerFactory, Logger log) throws IOException, ContentStreamException {
        InputStream itemInputStream = null;

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        try {
            itemInputStream = getContent(contentId, derferencerFactory);
        } finally {
            stopWatch.stop();
            if (itemInputStream == null) {
                logReponseTime(stopWatch, contentId.getIRIString(), "failed to resolve in", log);
            } else {
                logReponseTime(stopWatch, contentId.getIRIString(), "resolved in", log);
            }
        }
        if (itemInputStream == null) {
            throw new ContentStreamException("cannot generate Zenodo record due to unresolved content [" + contentId.getIRIString() + "]");
        }
        return itemInputStream;
    }

    static void logReponseTime(StopWatch stopWatch, String iriString, String msg, Logger log) {
        log.info("|" + iriString + "|" + msg + "|" + stopWatch.getTime(TimeUnit.MILLISECONDS) + "|ms|");
    }
}
