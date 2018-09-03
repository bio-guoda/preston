package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.process.RegistryReaderBioCASE;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.process.StatementLoggerNQuads;
import org.globalbioticinteractions.preston.process.StatementLoggerTSV;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.Archiver;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.Persistence;
import org.globalbioticinteractions.preston.store.StatementStoreImpl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.globalbioticinteractions.preston.RefNodeConstants.*;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toEnglishLiteral;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;

public abstract class CmdCrawl implements Runnable, Crawler {

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

    @Override
    public void run() {
        crawl(getCrawlMode());
    }

    protected void crawl(CrawlMode crawlMode) {


        CrawlContext ctx = new CrawlContext() {
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

        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    addAll(findCrawlInfo(ctx.getActivity(), ctx.getGraph(), ctx.getArchive()));
                    addAll(generateSeeds(ctx.getActivity()));
                }};

        File dataDir = new File("data");
        File tmpDir = new File(dataDir, "tmp");
        Persistence blobPersistence = new FilePersistence(
                tmpDir,
                new File(dataDir, "blob"));
        BlobStore blobStore = new AppendOnlyBlobStore(blobPersistence);

        StatementListener listeners[] = {
                new RegistryReaderIDigBio(blobStore, ctx, statementQueue::add),
                new RegistryReaderGBIF(blobStore, ctx, statementQueue::add),
                new RegistryReaderBioCASE(blobStore, ctx, statementQueue::add),
                getStatementLogger()
        };

        Persistence statementPersistence = new FilePersistence(tmpDir, new File(dataDir, "statement"));
        StatementListener archive = (CrawlMode.replay == crawlMode)
                ? createOfflineArchive(statementPersistence, blobStore, listeners, ctx)
                : createOnlineArchive(statementPersistence, blobStore, listeners, crawlMode, ctx);

        while (!statementQueue.isEmpty()) {
            archive.on(statementQueue.poll());
        }

        // wrapping up
        archive.on(toStatement(ctx.getActivity(), toIRI("http://www.w3.org/ns/prov#endedAtTime"), RefNodeFactory.nowLiteral()));
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
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#startedAtTime"), RefNodeFactory.nowLiteral()),
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

    ;

    private StatementListener getStatementLogger() {
        StatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLoggerTSV();
        } else if (Logger.nquads == logMode) {
            logger = new StatementLoggerNQuads();
        } else {
            logger = new StatementListener() {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(Triple statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        System.out.println();
                    } else {
                        System.out.print(".");
                    }
                }
            };
        }
        return logger;
    }

    private StatementListener createOfflineArchive(Persistence persistence, BlobStore blobStore, StatementListener listeners[], CrawlContext crawlContext) {
        StatementStoreImpl statementStore = new StatementStoreImpl(persistence) {
            @Override
            public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
            }
        };
        return new Archiver(blobStore, null, statementStore, crawlContext, listeners);
    }

    private StatementListener createOnlineArchive(Persistence persistence, BlobStore blobStore, StatementListener[] listener, CrawlMode crawlMode, CrawlContext crawlContext) {
        Archiver Archiver = new Archiver(blobStore, Resources::asInputStream, new StatementStoreImpl(persistence), crawlContext, listener);
        Archiver.setResolveOnMissingOnly(CrawlMode.resume == crawlMode);
        return Archiver;
    }

}
