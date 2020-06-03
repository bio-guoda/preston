package bio.guoda.preston.cmd;

import bio.guoda.preston.Preston;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.RegistryReaderALA;
import bio.guoda.preston.process.RegistryReaderBHL;
import bio.guoda.preston.process.RegistryReaderBioCASE;
import bio.guoda.preston.process.RegistryReaderDOI;
import bio.guoda.preston.process.RegistryReaderDataONE;
import bio.guoda.preston.process.RegistryReaderGBIF;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.RegistryReaderOBIS;
import bio.guoda.preston.process.RegistryReaderRSS;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.AGENT;
import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.PRESTON;
import static bio.guoda.preston.RefNodeConstants.SOFTWARE_AGENT;
import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public abstract class CmdActivity extends LoggingPersisting implements Runnable {
    private static final Log LOG = LogFactory.getLog(CmdActivity.class);

    private static final IRI ENTITY = toIRI("http://www.w3.org/ns/prov#Entity");
    private static final IRI ACTIVITY = toIRI("http://www.w3.org/ns/prov#Activity");
    public static final String PRESTON_DOI = "https://doi.org/10.5281/zenodo.1410543";
    public static final IRI PRESTON_DOI_IRI = toIRI(PRESTON_DOI);


    @Override
    public void run() {
        KeyValueStore blobKeyValueStore = getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory());

        BlobStore blobStore = new BlobStoreAppendOnly(blobKeyValueStore);

        KeyValueStore logRelationsStore = getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values());

        run(blobStore, new StatementStoreImpl(logRelationsStore));
    }


    protected void run(BlobStore blobStore, StatementStore logRelations) {
        ActivityContext ctx = createNewActivityContext(getActivityDescription());

        final ArchivingLogger archivingLogger = new ArchivingLogger(blobStore, logRelations, ctx);
        try {
            Runtime.getRuntime().addShutdownHook(new LoggerExitHook(archivingLogger));

            archivingLogger.start();

            final Queue<Quad> statementQueue =
                    new ConcurrentLinkedQueue<Quad>() {
                        {
                            addAll(findActivityInfo(ctx));
                            addPreviousVersionReference();
                        }

                        private void addPreviousVersionReference() throws IOException {
                            IRI mostRecentVersion = VersionUtil.findMostRecentVersion(getProvenanceRoot(), logRelations);
                            if (mostRecentVersion != null) {
                                add(toStatement(ctx.getActivity(), mostRecentVersion, USED_BY, ctx.getActivity()));
                            }
                        }
                    };

            initQueue(statementQueue, ctx);

            StatementListener printingLogger = StatementLogFactory.createPrintingLogger(getLogMode(), System.out);

            StatementListener[] listeners = Stream.concat(
                    createProcessors(blobStore, statementQueue::add),
                    createLoggers(archivingLogger, printingLogger)
            ).toArray(StatementListener[]::new);

            processQueue(statementQueue, blobStore, ctx, listeners);

            archivingLogger.stop();
        } catch (IOException ex) {
            LOG.warn("Crawl failed and was not archived.", ex);
        } finally {
            archivingLogger.destroy();
        }
    }

    abstract void initQueue(Queue<Quad> statementQueue, ActivityContext ctx);

    abstract void processQueue(Queue<Quad> statementQueue, BlobStore blobStore, ActivityContext ctx, StatementListener[] listeners);


    private Stream<StatementListener> createLoggers(ArchivingLogger archivingLogger, StatementListener printingLogger) {
        return Stream.of(
                printingLogger,
                archivingLogger);
    }

    private static Stream<StatementListener> createProcessors(BlobStore blobStore, StatementListener queueAsListener) {
        return Stream.of(
                new RegistryReaderIDigBio(blobStore, queueAsListener),
                new RegistryReaderGBIF(blobStore, queueAsListener),
                new RegistryReaderBioCASE(blobStore, queueAsListener),
                new RegistryReaderDataONE(blobStore, queueAsListener),
                new RegistryReaderRSS(blobStore, queueAsListener),
                new RegistryReaderBHL(blobStore, queueAsListener),
                new RegistryReaderOBIS(blobStore, queueAsListener),
                new RegistryReaderDOI(blobStore, queueAsListener),
                new RegistryReaderALA(blobStore, queueAsListener)
        );
    }


    private ActivityContext createNewActivityContext(String activityDescription) {
        return new ActivityContext() {
            private final IRI activity = toIRI(UUID.randomUUID());

            @Override
            public IRI getActivity() {
                return activity;
            }

            @Override
            public String getDescription() { return activityDescription; };

        };
    }
    static List<Quad> findActivityInfo(ActivityContext activity) {

        IRI crawlActivity = activity.getActivity();


        String version = Preston.getVersion(null);
        String versionString = version == null ? "" : (" (Version " + Preston.getVersion() + ")");
        return Arrays.asList(
                toStatement(crawlActivity, PRESTON, IS_A, SOFTWARE_AGENT),
                toStatement(crawlActivity, PRESTON, IS_A, AGENT),
                toStatement(crawlActivity, PRESTON, DESCRIPTION, toEnglishLiteral("Preston is a software program that finds, archives and provides access to biodiversity datasets.")),


                toStatement(crawlActivity, crawlActivity, IS_A, ACTIVITY),
                toStatement(crawlActivity, crawlActivity, DESCRIPTION, toEnglishLiteral(activity.getDescription())),
                toStatement(crawlActivity, crawlActivity, toIRI("http://www.w3.org/ns/prov#startedAtTime"), RefNodeFactory.nowDateTimeLiteral()),
                toStatement(crawlActivity, crawlActivity, toIRI("http://www.w3.org/ns/prov#wasStartedBy"), PRESTON),


                toStatement(crawlActivity, PRESTON_DOI_IRI, USED_BY, crawlActivity),
                toStatement(crawlActivity, PRESTON_DOI_IRI, IS_A, toIRI("http://purl.org/dc/dcmitype/Software")),
                toStatement(crawlActivity, PRESTON_DOI_IRI,
                        toIRI("http://purl.org/dc/terms/bibliographicCitation"),
                        toEnglishLiteral("Jorrit Poelen, Icaro Alzuru, & Michael Elliott. 2019. Preston: a biodiversity dataset tracker" + versionString + " [Software]. Zenodo. http://doi.org/10.5281/zenodo.1410543")),

                toStatement(crawlActivity, BIODIVERSITY_DATASET_GRAPH, IS_A, ENTITY),
                toStatement(crawlActivity, BIODIVERSITY_DATASET_GRAPH, DESCRIPTION, toEnglishLiteral("A biodiversity dataset graph archive."))
        );
    }

    abstract String getActivityDescription();

    private static class LoggerExitHook extends Thread {

        private final ArchivingLogger archivingLogger;

        public LoggerExitHook(ArchivingLogger archivingLogger) {
            this.archivingLogger = archivingLogger;
        }

        public void run() {
            try {
                archivingLogger.stop();
            } catch (IOException e) {
                LOG.warn("failed to stop archive logger", e);
            } finally {
                archivingLogger.destroy();
            }
        }
    }

    private class ArchivingLogger implements StatementListener {
        private final BlobStore blobStore;
        private final StatementStore statementStore;
        private final ActivityContext ctx;
        File tmpArchive;
        PrintStream printStream;
        StatementListener listener;

        public ArchivingLogger(BlobStore blobStore, StatementStore statementStore, ActivityContext ctx) {
            this.blobStore = blobStore;
            this.statementStore = statementStore;
            this.ctx = ctx;
            tmpArchive = null;
            printStream = null;
            listener = null;
        }

        @Override
        public void on(Quad statement) {
            if (listener != null) {
                listener.on(statement);
            }
        }

        void start() throws IOException {
            tmpArchive = File.createTempFile("archive", "nq", getTmpDir());
            printStream = new PrintStream(IOUtils.buffer(new FileOutputStream(tmpArchive)), true, StandardCharsets.UTF_8.name());
            listener = new StatementLoggerNQuads(printStream);
        }

        void stop() throws IOException {
            if (tmpArchive != null && tmpArchive.exists() && printStream != null && listener != null) {
                printStream.flush();
                printStream.close();

                printStream = null;

                try (FileInputStream is = new FileInputStream(tmpArchive)) {
                    IRI newVersion = blobStore.put(is);
                    RefNodeFactory.nowDateTimeLiteral();

                    IRI previousVersion = VersionUtil.findMostRecentVersion(getProvenanceRoot(), statementStore);
                    if (previousVersion == null) {
                        statementStore.put(Pair.of(BIODIVERSITY_DATASET_GRAPH, HAS_VERSION), newVersion);
                    } else {
                        statementStore.put(Pair.of(HAS_PREVIOUS_VERSION, previousVersion), newVersion);
                    }
                }
            }

        }

        void destroy() {
            if (printStream != null) {
                printStream.flush();
                printStream.close();
            }
            if (tmpArchive != null) {
                FileUtils.deleteQuietly(tmpArchive);
            }
        }
    }
}
