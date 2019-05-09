package bio.guoda.preston.cmd;

import bio.guoda.preston.Resources;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.RegistryReaderBHL;
import bio.guoda.preston.process.RegistryReaderBioCASE;
import bio.guoda.preston.process.RegistryReaderDataONE;
import bio.guoda.preston.process.RegistryReaderGBIF;
import bio.guoda.preston.process.RegistryReaderIDigBio;
import bio.guoda.preston.process.RegistryReaderRSS;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.KeyGeneratingStream;
import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreImpl;
import bio.guoda.preston.store.VersionUtil;
import com.beust.jcommander.Parameter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static bio.guoda.preston.RefNodeConstants.AGENT;
import static bio.guoda.preston.RefNodeConstants.ARCHIVE;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.PRESTON;
import static bio.guoda.preston.RefNodeConstants.SOFTWARE_AGENT;
import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public abstract class CmdCrawl extends LoggingPersisting implements Runnable, Crawler {
    private static final Log LOG = LogFactory.getLog(CmdCrawl.class);

    public static final IRI ENTITY = toIRI("http://www.w3.org/ns/prov#Entity");
    public static final IRI ACTIVITY = toIRI("http://www.w3.org/ns/prov#Activity");

    @Parameter(names = {"-u", "--seed-uris"}, description = "starting points for graph discovery. Only active when no content urls are provided.", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.IDIGBIO.getIRIString());
        add(Seeds.GBIF.getIRIString());
        add(Seeds.BIOCASE.getIRIString());
        //add(Seeds.DATA_ONE.getIRIString());
    }};

    @Parameter(description = "[url1] [url2] ...",
            validateWith = IRIValidator.class)
    private List<String> IRIs = new ArrayList<>();


    @Override
    public void run() {
        KeyValueStore blobKeyValueStore = getKeyValueStore();

        BlobStore blobStore = new BlobStoreAppendOnly(blobKeyValueStore);

        KeyValueStore logRelationsStore = getKeyValueStore();

        run(blobStore, new StatementStoreImpl(new NullKeyValueStore()), new StatementStoreImpl(logRelationsStore));
    }


    protected void run(BlobStore blobStore, StatementStore datasetRelations, StatementStore logRelations) {
        CrawlContext ctx = createNewCrawlContext();

        final ArchivingLogger archivingLogger = new ArchivingLogger(blobStore, logRelations, ctx);
        try {
            Runtime.getRuntime().addShutdownHook(new LoggerExitHook(archivingLogger));

            archivingLogger.start();

            final Queue<Triple> statementQueue =
                    new ConcurrentLinkedQueue<Triple>() {
                        {
                            addAll(findCrawlInfo(ctx.getActivity()));
                            addPreviousVersionReference();
                        }

                        private void addPreviousVersionReference() throws IOException {
                            IRI mostRecentVersion = VersionUtil.findMostRecentVersion(ARCHIVE, logRelations);
                            if (mostRecentVersion != null) {
                                add(toStatement(mostRecentVersion, USED_BY, ctx.getActivity()));
                            }
                        }
                    };

            if (IRIs.isEmpty()) {
                statementQueue.addAll(generateSeeds(ctx.getActivity()));
            } else {
                IRIs.forEach(iri -> statementQueue.add(toStatement(toIRI(iri), HAS_VERSION, toBlank())));
            }

            doCrawl(blobStore, ctx, statementQueue, archivingLogger, datasetRelations);

            archivingLogger.stop();
        } catch (IOException ex) {
            LOG.warn("Crawl failed and was not archived.", ex);
        } finally {
            archivingLogger.destroy();
        }
    }

    private void doCrawl(BlobStore blobStore, CrawlContext ctx, Queue<Triple> statementQueue, StatementListener statementLoggerNQuads, StatementStore statementStore) {
        StatementListener[] listeners = {
                new RegistryReaderIDigBio(blobStore, statementQueue::add),
                new RegistryReaderGBIF(blobStore, statementQueue::add),
                new RegistryReaderBioCASE(blobStore, statementQueue::add),
                new RegistryReaderDataONE(blobStore, statementQueue::add),
                new RegistryReaderRSS(blobStore, statementQueue::add),
                new RegistryReaderBHL(blobStore, statementQueue::add),
                StatementLogFactory.createLogger(getLogMode()),
                statementLoggerNQuads
        };

        StatementListener archive = createOnlineArchive(blobStore, listeners, getCrawlMode(), ctx, statementStore);

        while (!statementQueue.isEmpty()) {
            archive.on(statementQueue.poll());
        }

    }


    private CrawlContext createNewCrawlContext() {
        return new CrawlContext() {
            private final IRI crawlActivity = toIRI(UUID.randomUUID());

            @Override
            public IRI getActivity() {
                return crawlActivity;
            }

        };
    }

    private List<Triple> generateSeeds(final IRI crawlActivity) {
        return seedUrls.stream()
                .map((String uriString) -> toStatement(toIRI(uriString), WAS_ASSOCIATED_WITH, crawlActivity))
                .collect(Collectors.toList());
    }

    static List<Triple> findCrawlInfo(IRI crawlActivity) {

        return Arrays.asList(
                toStatement(PRESTON, IS_A, SOFTWARE_AGENT),
                toStatement(PRESTON, IS_A, AGENT),
                toStatement(PRESTON, DESCRIPTION, toEnglishLiteral("Preston is a software program that finds, archives and provides access to biodiversity datasets.")),


                toStatement(crawlActivity, IS_A, ACTIVITY),
                toStatement(crawlActivity, DESCRIPTION, toEnglishLiteral("A crawl event that discovers biodiversity archives.")),
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#startedAtTime"), RefNodeFactory.nowDateTimeLiteral()),
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#wasStartedBy"), PRESTON),

                toStatement(ARCHIVE, IS_A, ENTITY),
                toStatement(ARCHIVE, DESCRIPTION, toEnglishLiteral("A biodiversity dataset graph archive."))
        );
    }


    private StatementListener createOnlineArchive(BlobStore blobStore, StatementListener[] listener, CrawlMode crawlMode, CrawlContext crawlContext, StatementStore statementStore) {
        Archiver Archiver = new Archiver(blobStore, Resources::asInputStream, statementStore, crawlContext, listener);
        Archiver.setResolveOnMissingOnly(CrawlMode.resume == crawlMode);
        return Archiver;
    }

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

    private static class NullKeyValueStore implements KeyValueStore {
        @Override
        public void put(String key, String value) throws IOException {

        }

        @Override
        public String put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
            if (is != null) {
                is.close();
            }
            return null;
        }

        @Override
        public void put(String key, InputStream is) throws IOException {
            if (is != null) {
                is.close();
            }
        }

        @Override
        public InputStream get(String key) throws IOException {
            return null;
        }
    }

    private class ArchivingLogger implements StatementListener {
        private final BlobStore blobStore;
        private final StatementStore statementStore;
        private final CrawlContext ctx;
        File tmpArchive;
        PrintStream printStream;
        StatementListener listener;

        public ArchivingLogger(BlobStore blobStore, StatementStore statementStore, CrawlContext ctx) {
            this.blobStore = blobStore;
            this.statementStore = statementStore;
            this.ctx = ctx;
            tmpArchive = null;
            printStream = null;
            listener = null;
        }

        @Override
        public void on(Triple statement) {
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

                IRI newVersion = blobStore.putBlob(new FileInputStream(tmpArchive));
                RefNodeFactory.nowDateTimeLiteral();

                IRI previousVersion = VersionUtil.findMostRecentVersion(ARCHIVE, statementStore);
                if (previousVersion == null) {
                    statementStore.put(Pair.of(ARCHIVE, HAS_VERSION), newVersion);
                } else {
                    statementStore.put(Pair.of(HAS_PREVIOUS_VERSION, previousVersion), newVersion);
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
