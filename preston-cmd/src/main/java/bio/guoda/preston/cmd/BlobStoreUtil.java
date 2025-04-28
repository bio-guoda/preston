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
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.stream.ContentHashDereferencer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.mapdb.DB;
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
        Pair<Map<String, String>, Map<String, String>> aliasAndVersionMaps = buildIndexedBlobStore(persisting);
        Map<String, String> versionMap = aliasAndVersionMaps.getKey();
        Map<String, String> aliasMap = aliasAndVersionMaps.getValue();

        return new BlobStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                IRI iriForLookup = null;
                if (HashKeyUtil.isValidHashKey(uri)) {
                    iriForLookup = uri;
                } else {
                    String indexedVersion = versionMap.get(uri.getIRIString());
                    if (StringUtils.isBlank(indexedVersion)) {
                        String alternate = aliasMap.get(uri.getIRIString());
                        indexedVersion = StringUtils.isBlank(alternate) ? uri.getIRIString() : versionMap.get(alternate);
                    }
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


    private static org.apache.commons.lang3.tuple.Pair<Map<String, String>, Map<String, String>> buildIndexedBlobStore(Persisting persisting) {

        File tmpDir = new File(persisting.getTmpDir());
        IRI provenanceAnchor = AnchorUtil.findAnchorOrThrow(persisting);

        // indexing
        DB db = newTmpFileDB(tmpDir)
                .deleteFilesAfterClose()
                .closeOnJvmShutdown()
                .transactionDisable()
                .make();

        Map<String, String> versionMap = db
                .createTreeMap("versionMap")
                .make();

        Map<String, String> alternateMap = db
                .createTreeMap("alternateMap")
                .make();

        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        AtomicLong index = new AtomicLong(0);
        LOG.info("version index for [" + provenanceAnchor + "] building...");

        StatementsListener listener = new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                if (hasIRIs(statement)) {
                    IRI object = (IRI) statement.getObject();
                    IRI subject = (IRI) statement.getSubject();
                    if (RefNodeConstants.HAS_VERSION.equals(statement.getPredicate())
                            && !RefNodeFactory.isBlankOrSkolemizedBlank(statement.getObject())) {
                        indexVersion(statement, object);
                    } else if (RefNodeConstants.ALTERNATE_OF.equals(statement.getPredicate())) {
                        indexAlternate(object, subject);
                    }
                }
            }

            void indexAlternate(IRI object, IRI subject) {
                alternateMap.putIfAbsent(object.getIRIString(), subject.getIRIString());
                alternateMap.putIfAbsent(subject.getIRIString(), object.getIRIString());
            }

            void indexVersion(Quad statement, IRI object) {
                IRI version = object;
                if (HashKeyUtil.isValidHashKey(version)) {
                    index.incrementAndGet();
                    String uri = ((IRI) statement.getSubject()).getIRIString();
                    String indexedVersion = version.getIRIString();
                    versionMap.putIfAbsent(uri, indexedVersion);
                }
            }

            public boolean hasIRIs(Quad statement) {
                return statement.getSubject() instanceof IRI && statement.getObject() instanceof IRI;
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

        return org.apache.commons.lang3.tuple.Pair.of(versionMap, alternateMap);
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
