package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyQuad;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.ParsingEmitter;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ContentHashDereferencer;
import bio.guoda.preston.store.HashKeyUtil;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.mapdb.DBMaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

import java.io.File;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@CommandLine.Command(
        name = "zotero-stream",
        description = "Stream Zotero records into line-json with Zenodo metadata"
)
public class CmdZoteroStream extends LoggingPersisting implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(CmdZoteroStream.class);


    @CommandLine.Option(
            names = {"--community", "--communities"},
            split = ",",
            description = "select which Zenodo communities to submit to. If community is known (e.g., batlit, taxodros), default metadata is included."
    )

    private List<String> communities = new ArrayList<>();


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        BlobStoreReadOnly blobStoreReadOnly = new BlobStoreReadOnly() {

            private ContentHashDereferencer contentHashDereferencer = new ContentHashDereferencer(blobStoreAppendOnly);

            @Override
            public InputStream get(IRI uri) throws IOException {
                return contentHashDereferencer.get(uri);
            }
        };
        run(blobStoreReadOnly);

    }

    private static DBMaker newTmpFileDB(File tmpDir) {
        try {
            File db = File.createTempFile("mapdb-temp", "db", tmpDir);
            return DBMaker.newFileDB(db);
        } catch (IOException e) {
            throw new IOError(new IOException("failed to create tmpFile in [" + tmpDir.getAbsolutePath() + "]", e));
        }

    }


    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        Map<String, String> treeMap = buildIndexedBlobStore(blobStoreReadOnly, this);

        BlobStoreReadOnly blobStoreWithIndexedVersions = new BlobStoreReadOnly() {

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


        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.INSTANCE,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);


        StatementsListener textMatcher = new ZoteroFileExtractor(
                this,
                blobStoreWithIndexedVersions,
                getOutputStream(),
                communities,
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamOfAnyVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

    public static Map<String, String> buildIndexedBlobStore(BlobStoreReadOnly blobStoreReadOnly,
                                                            Persisting persisting) {

        File tmpDir = persisting.getTmpDir();
        IRI provenanceAnchor = persisting.getProvenanceAnchor();
        if (PROVENANCE_ANCHOR_DEFAULT.equals(provenanceAnchor)) {
            throw new IllegalArgumentException("--anchor provenance anchor not set; please set provenance anchor");
        }
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

}

