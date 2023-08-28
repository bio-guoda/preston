package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.Version;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.HexaStore;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.ValidatingKeyValueStreamHashTypeIRIFactory;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.AGENT;
import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.PRESTON;
import static bio.guoda.preston.RefNodeConstants.SOFTWARE_AGENT;
import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public abstract class CmdActivity extends LoggingPersisting implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CmdActivity.class);

    private static final IRI ENTITY = toIRI("http://www.w3.org/ns/prov#Entity");
    private static final IRI ACTIVITY = toIRI("http://www.w3.org/ns/prov#Activity");
    public static final String PRESTON_DOI = "https://doi.org/10.5281/zenodo.1410543";
    public static final IRI PRESTON_DOI_IRI = toIRI(PRESTON_DOI);


    @Override
    public void run() {
        BlobStore blobStore = createAppendOnlyStore(getKeyValueStore(
                new ValidatingKeyValueStreamContentAddressedFactory()
        ));

        BlobStore provStore = createAppendOnlyStore(getKeyValueStore(
                new ValidatingKeyValueStreamContentAddressedFactory()
        ));

        KeyValueStore provenanceIndex = getKeyValueStore(
                new ValidatingKeyValueStreamHashTypeIRIFactory()
        );

        run(blobStore, provStore, new HexaStoreImpl(provenanceIndex, getHashType()));
    }

    private BlobStore createAppendOnlyStore(KeyValueStore blobKeyValueStore) {
        return new BlobStoreAppendOnly(
                    blobKeyValueStore,
                    true,
                    getHashType()
            );
    }


    protected void run(BlobStore blobStore, BlobStore provStore, HexaStore provIndex) {
        ActivityContext ctx = createNewActivityContext(getActivityDescription());

        final ArchivingLogger archivingLogger = new ArchivingLogger(this, provStore, provIndex, ctx);
        try {
            Runtime.getRuntime().addShutdownHook(new LoggerExitHook(archivingLogger));

            archivingLogger.start();

            final Queue<List<Quad>> statementQueue =
                    new ConcurrentLinkedQueue<List<Quad>>() {
                        {
                            add(findActivityInfo(ctx));
                            addProvenanceRoots();
                        }

                        private void addProvenanceRoots() throws IOException {
                            AtomicReference<IRI> head = AnchorUtil.findHead(CmdActivity.this);
                            if (head.get() != null) {
                                add(Collections.singletonList(toStatement(ctx.getActivity(), head.get(), USED_BY, ctx.getActivity())));
                            }
                        }
                    };

            initQueue(statementQueue, ctx);

            StatementsListener[] listeners = initListeners(blobStore, archivingLogger, statementQueue);

            processQueue(statementQueue, blobStore, ctx, listeners);

            archivingLogger.stop();
        } catch (IOException ex) {
            LOG.warn("Crawl failed and was not archived.", ex);
        } finally {
            archivingLogger.destroy();
        }
    }

    protected StatementsListener[] initListeners(BlobStoreReadOnly blobStore,
                                                 StatementsListener archivingLogger,
                                                 Queue<List<Quad>> statementQueue) {

        return createStatementListeners(
                resolvingBlobStore(blobStore),
                archivingLogger,
                statementQueue
        );
    }

    private StatementsListener[] createStatementListeners(BlobStoreReadOnly blobStore, StatementsListener archivingLogger, Queue<List<Quad>> statementQueue) {
        StatementsListener printingLogger = StatementLogFactory
                .createPrintingLogger(
                        getLogMode(),
                        getOutputStream(),
                        this);

        Stream<StatementsListener> processors = createProcessors(blobStore, new StatementsListener() {
            @Override
            public void on(Quad statement) {
                on(Collections.singletonList(statement));
            }

            @Override
            public void on(List<Quad> statements) {
                statementQueue.add(statements);
            }
        });
        return Stream.concat(
                processors,
                createLoggers(archivingLogger, printingLogger)
        ).toArray(StatementsListener[]::new);
    }

    abstract void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx);

    abstract void processQueue(Queue<List<Quad>> statementQueue,
                               BlobStore blobStore,
                               ActivityContext ctx,
                               StatementsListener[] listeners);


    private Stream<StatementsListener> createLoggers(StatementsListener archivingLogger, StatementsListener printingLogger) {
        return Stream.of(
                printingLogger,
                archivingLogger);
    }

    protected Stream<StatementsListener> createProcessors(BlobStoreReadOnly blobStore, StatementsListener queueAsListener) {
        return Stream.empty();
    }


    private ActivityContext createNewActivityContext(String activityDescription) {
        return new ActivityContext() {
            private final IRI activity = toIRI(UUID.randomUUID());

            @Override
            public IRI getActivity() {
                return activity;
            }

            @Override
            public String getDescription() {
                return activityDescription;
            }

            ;

        };
    }

    static List<Quad> findActivityInfo(ActivityContext activity) {

        IRI crawlActivity = activity.getActivity();


        String version = Version.getVersionString(null);
        String versionString = version == null ? "" : (" (Version " + Version.getVersionString() + ")");
        return Arrays.asList(
                toStatement(crawlActivity, PRESTON, IS_A, SOFTWARE_AGENT),
                toStatement(crawlActivity, PRESTON, IS_A, AGENT),
                toStatement(crawlActivity, PRESTON, DESCRIPTION, toEnglishLiteral("Preston is a software program that finds, archives and provides access to biodiversity datasets.")),


                toStatement(crawlActivity, crawlActivity, IS_A, ACTIVITY),
                toStatement(crawlActivity, crawlActivity, DESCRIPTION, toEnglishLiteral(activity.getDescription())),
                toStatement(crawlActivity, crawlActivity, toIRI("http://www.w3.org/ns/prov#startedAtTime"), RefNodeFactory.nowDateTimeLiteral()),
                toStatement(crawlActivity, crawlActivity, RefNodeConstants.WAS_STARTED_BY, PRESTON),


                toStatement(crawlActivity, PRESTON_DOI_IRI, USED_BY, crawlActivity),
                toStatement(crawlActivity, PRESTON_DOI_IRI, IS_A, toIRI("http://purl.org/dc/dcmitype/Software")),
                toStatement(crawlActivity, PRESTON_DOI_IRI,
                        toIRI("http://purl.org/dc/terms/bibliographicCitation"),
                        toEnglishLiteral("Jorrit Poelen, Icaro Alzuru, & Michael Elliott. 2021. Preston: a biodiversity dataset tracker" + versionString + " [Software]. Zenodo. http://doi.org/10.5281/zenodo.1410543")),

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

}
