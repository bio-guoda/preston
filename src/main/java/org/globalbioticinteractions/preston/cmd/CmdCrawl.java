package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.StatementLogFactory;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.process.RegistryReaderBioCASE;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.process.StatementLoggerNQuads;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.Archiver;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.Persistence;
import org.globalbioticinteractions.preston.store.StatementStoreImpl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;

import static org.globalbioticinteractions.preston.RefNodeConstants.AGENT;
import static org.globalbioticinteractions.preston.RefNodeConstants.ARCHIVE_COLLECTION_IRI;
import static org.globalbioticinteractions.preston.RefNodeConstants.COLLECTION;
import static org.globalbioticinteractions.preston.RefNodeConstants.DESCRIPTION;
import static org.globalbioticinteractions.preston.RefNodeConstants.GENERATED_AT_TIME;
import static org.globalbioticinteractions.preston.RefNodeConstants.GRAPH_COLLECTION_IRI;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.IS_A;
import static org.globalbioticinteractions.preston.RefNodeConstants.PRESTON;
import static org.globalbioticinteractions.preston.RefNodeConstants.SOFTWARE_AGENT;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_GENERATED_BY;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toEnglishLiteral;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;

public abstract class CmdCrawl implements Runnable, Crawler {
    private static final Log LOG = LogFactory.getLog(CmdCrawl.class);

    public static final IRI ENTITY = toIRI("http://www.w3.org/ns/prov#Entity");
    public static final IRI ACTIVITY = toIRI("http://www.w3.org/ns/prov#Activity");

    @Parameter(names = {"-u", "--seed-uris"}, description = "[starting points of graph crawl (aka seed URIs)]", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.IDIGBIO.getIRIString());
        add(Seeds.GBIF.getIRIString());
        add(Seeds.BIOCASE.getIRIString());
    }};

    @Parameter(names = {"-l", "--log",}, description = "select how to show the biodiversity graph", converter = LoggerConverter.class)
    private Logger logMode = Logger.tsv;

    protected Logger getLogMode() {
        return logMode;
    }

    @Override
    public void run() {
        File dataDir = getDataDir();
        File tmpDir = getTmpDir();

        Persistence blobPersistence = new FilePersistence(
                tmpDir,
                new File(dataDir, "blob"));

        BlobStore blobStore = new AppendOnlyBlobStore(blobPersistence);

        Persistence statementPersistence = new FilePersistence(tmpDir, new File(dataDir, "statement"));

        run(blobStore, statementPersistence);
    }

    private File getTmpDir() {
        return new File(getDataDir(), "tmp");
    }

    private File getDataDir() {
        return new File("data");
    }

    protected void run(BlobStore blobStore, Persistence statementPersistence) {
        CrawlContext ctx = createNewCrawlContext();

        List<Triple> crawlInfo = findCrawlInfo(ctx.getActivity(), ctx.getGraph(), ctx.getArchive());

        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    addAll(crawlInfo);
                    addAll(generateSeeds(ctx.getActivity()));
                }};


        File tmpArchive;
        PrintStream printStream;
        try {
            tmpArchive = File.createTempFile("archive", "nq", getTmpDir());
            printStream = new PrintStream(IOUtils.buffer(new FileOutputStream(tmpArchive)), true, StandardCharsets.UTF_8.name());
        } catch (IOException e) {
            throw new RuntimeException("failed to create tmp file", e);
        }

        StatementListener listeners[] = {
                new RegistryReaderIDigBio(blobStore, statementQueue::add),
                new RegistryReaderGBIF(blobStore, statementQueue::add),
                new RegistryReaderBioCASE(blobStore, statementQueue::add),
                StatementLogFactory.createLogger(logMode),
                new StatementLoggerNQuads(printStream)
        };

        StatementListener archive = createOnlineArchive(statementPersistence, blobStore, listeners, getCrawlMode(), ctx);

        while (!statementQueue.isEmpty()) {
            archive.on(statementQueue.poll());
        }

        try {
            // wrapping up
            archive.on(toStatement(ctx.getActivity(), toIRI("http://www.w3.org/ns/prov#endedAtTime"), RefNodeFactory.nowDateTimeLiteral()));

            printStream.flush();
            printStream.close();
            IRI archiveIRI = blobStore.putBlob(new FileInputStream(tmpArchive));

            archive.on(toStatement(ARCHIVE_COLLECTION_IRI, HAS_VERSION, archiveIRI));

            archive.on(toStatement(archiveIRI, WAS_GENERATED_BY, ctx.getActivity()));
            archive.on(toStatement(archiveIRI, GENERATED_AT_TIME, RefNodeFactory.nowDateTimeLiteral()));

        } catch (IOException ex) {
            LOG.warn("failed to archive crawl log write to [" + tmpArchive + "]", ex);
        }
    }


    private CrawlContext createNewCrawlContext() {
        return new CrawlContext() {
            private final IRI crawlActivity = toIRI(UUID.randomUUID());
            private final IRI biodiversityGraph = toIRI(UUID.randomUUID());
            private final IRI biodiversityArchive = toIRI(UUID.randomUUID());

            @Override
            public IRI getActivity() {
                return crawlActivity;
            }

            @Override
            public IRI getArchive() {
                return biodiversityArchive;
            }

            @Override
            public IRI getGraph() {
                return biodiversityGraph;
            }

        };
    }

    private List<Triple> generateSeeds(final IRI crawlActivity) {
        return seedUrls.stream()
                .map((String uriString) -> toStatement(toIRI(uriString), WAS_ASSOCIATED_WITH, crawlActivity))
                .collect(Collectors.toList());
    }

    static List<Triple> findCrawlInfo(IRI crawlActivity, IRI biodiversityGraph, IRI biodiversityArchive) {
        // new crawl activity created for each crawl

        IRI biodiversityGraphCollection = GRAPH_COLLECTION_IRI;

        IRI biodiversityArchiveCollection = ARCHIVE_COLLECTION_IRI;

        IRI crawler = PRESTON;

        return Arrays.asList(
                toStatement(crawler, IS_A, SOFTWARE_AGENT),
                toStatement(crawler, IS_A, AGENT),
                toStatement(crawler, DESCRIPTION, toEnglishLiteral("Preston is a software program that finds, archives and provides access to biodiversity datasets.")),


                toStatement(crawlActivity, IS_A, ACTIVITY),
                toStatement(crawlActivity, DESCRIPTION, toEnglishLiteral("A crawl event is an activity that discovers biodiversity archives.")),
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#startedAtTime"), RefNodeFactory.nowDateTimeLiteral()),
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#wasStartedBy"), crawler),

                toStatement(biodiversityGraphCollection, IS_A, ENTITY),
                toStatement(biodiversityGraphCollection, IS_A, COLLECTION),
                toStatement(biodiversityGraphCollection, DESCRIPTION, toEnglishLiteral("A collection of biodiversity graphs.")),
                toStatement(biodiversityGraphCollection, WAS_GENERATED_BY, crawlActivity),

                toStatement(biodiversityGraph, IS_A, ENTITY),
                toStatement(biodiversityGraph, IS_A, COLLECTION),
                toStatement(biodiversityGraph, DESCRIPTION, toEnglishLiteral("A version of the biodiversity graph.")),
                toStatement(biodiversityGraph, WAS_GENERATED_BY, crawlActivity),

                toStatement(biodiversityArchiveCollection, IS_A, ENTITY),
                toStatement(biodiversityArchiveCollection, DESCRIPTION, toEnglishLiteral("A collection of biodiversity graph archives.")),
                toStatement(biodiversityArchiveCollection, WAS_GENERATED_BY, crawlActivity),
                toStatement(biodiversityArchiveCollection, toIRI("http://www.w3.org/ns/prov#wasDerivedFrom"), biodiversityGraph),

                toStatement(biodiversityArchive, IS_A, ENTITY),
                toStatement(biodiversityArchive, DESCRIPTION, toEnglishLiteral("An nquad archive of the version of this biodiversity graph.")),
                toStatement(biodiversityArchive, WAS_GENERATED_BY, crawlActivity),
                toStatement(biodiversityArchive, toIRI("http://www.w3.org/ns/prov#wasDerivedFrom"), biodiversityGraph)


        );
    }


    private StatementListener createOnlineArchive(Persistence persistence, BlobStore blobStore, StatementListener[] listener, CrawlMode crawlMode, CrawlContext crawlContext) {
        Archiver Archiver = new Archiver(blobStore, Resources::asInputStream, new StatementStoreImpl(persistence), crawlContext, listener);
        Archiver.setResolveOnMissingOnly(CrawlMode.resume == crawlMode);
        return Archiver;
    }

}
