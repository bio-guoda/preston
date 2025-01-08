package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.AliasDereferencer;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ContentHashDereferencer;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

public class BlobStoreUtil {
    private static final Logger LOG = LoggerFactory.getLogger(BlobStoreUtil.class);

    public static BlobStoreReadOnly createIndexedBlobStoreFor(BlobStoreReadOnly blobStoreReadOnly, Persisting persisting) {
        Map<String, String> treeMap = buildIndexedBlobStore(persisting);

        return new BlobStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                IRI iriForLookup = null;
                if (HashKeyUtil.isValidHashKey(uri)) {
                    iriForLookup = uri;
                } else {
                    String indexedVersion = treeMap.get(uri.getIRIString());
                    iriForLookup = StringUtils.isBlank(indexedVersion) ? uri : RefNodeFactory.toIRI(indexedVersion);
                }

                if (iriForLookup == null) {
                    throw new IOException("failed to find content associated to [" + uri + "] in index.");
                }

                return blobStoreReadOnly.get(iriForLookup);
            }
        };
    }

    public static BlobStoreReadOnly createResolvingBlobStoreFor(Dereferencer<InputStream> blobStore, Persisting persisting) {
        return new AliasDereferencer(
                new ContentHashDereferencer(blobStore),
                persisting,
                persisting.getProvenanceTracer()
        );
    }


    private static Map<String, String> buildIndexedBlobStore(Persisting persisting) {

        File tmpDir = new File(persisting.getTmpDir());
        IRI provenanceAnchor = AnchorUtil.findAnchorOrThrow(persisting);

        // indexing
        DBMaker maker = newTmpFileDB(tmpDir);
        Map<String, String> treeMap = maker
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make()
                .createTreeMap("zotero-stream")
                .make();


        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        AtomicLong index = new AtomicLong(0);
        LOG.info("version index for [" + provenanceAnchor + "] building...");

        StatementsListener listener = new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                if (RefNodeConstants.HAS_VERSION.equals(statement.getPredicate())
                        && !RefNodeFactory.isBlankOrSkolemizedBlank(statement.getObject())) {
                    if (statement.getSubject() instanceof IRI && statement.getObject() instanceof IRI) {
                        IRI version = (IRI) statement.getObject();
                        if (HashKeyUtil.isValidHashKey(version)) {
                            index.incrementAndGet();
                            String uri = ((IRI) statement.getSubject()).getIRIString();
                            String indexedVersion = version.getIRIString();
                            treeMap.putIfAbsent(uri, indexedVersion);
                        }
                    }
                }

            }
        };
        ReplayUtil.replay(listener, persisting, new EmittingStreamFactory() {
            @Override
            public ParsingEmitter createEmitter(StatementEmitter emitter, ProcessorState context) {
                return new EmittingStreamOfAnyQuad(emitter, context);
            }
        });
        stopWatch.stop();
        LOG.info("version index for [" + provenanceAnchor + "] with [" + index.get() + "] versions built in [" + stopWatch.getTime(TimeUnit.SECONDS) + "] s");

        return treeMap;
    }

    private static DBMaker newTmpFileDB(File tmpDir) {
        try {
            File db = File.createTempFile("mapdb-temp", "db", tmpDir);
            return DBMaker.newFileDB(db);
        } catch (IOException e) {
            throw new IOError(new IOException("failed to create tmpFile in [" + tmpDir.getAbsolutePath() + "]", e));
        }

    }

}
