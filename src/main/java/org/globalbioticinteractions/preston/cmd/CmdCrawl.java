package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.RefNodeConstants;
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

import static org.globalbioticinteractions.preston.model.RefNodeFactory.toEnglishLiteral;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toUUID;

public abstract class CmdCrawl implements Runnable, Crawler {

    public static final IRI ENTITY = toIRI("http://www.w3.org/ns/prov#Entity");
    public static final IRI ACTIVITY = toIRI("http://www.w3.org/ns/prov#Activity");
    public static final IRI AGENT = toIRI("http://www.w3.org/ns/prov#Agent");
    public static final IRI SOFTWARE_AGENT = toIRI("http://www.w3.org/ns/prov#SoftwareAgent");
    public static final IRI DESCRIPTION = toIRI("http://purl.org/dc/terms/description");
    public static final IRI GENERATED_BY = toIRI("http://www.w3.org/ns/prov#wasGeneratedBy");
    public static final IRI COLLECTION = toIRI("http://www.w3.org/ns/prov#Collection");
    public static final IRI ORGANIZATION = toIRI("http://www.w3.org/ns/prov#Organization");
    public static final IRI WAS_ASSOCIATED_WITH = toIRI("http://www.w3.org/ns/prov#wasAssociatedWith");

    public static final IRI GRAPH_COLLECTION_IRI = toUUID(RefNodeConstants.GRAPH_COLLECTION.toString());
    public static final IRI ARCHIVE_COLLECTION_IRI = toUUID(RefNodeConstants.ARCHIVE_COLLECTION.toString());

    public static final IRI IS_A = toIRI("http://www.w3.org/1999/02/22-rdf-syntax-ns#type");
    public static final IRI CREATED_BY = toIRI("http://purl.org/pav/createdBy");

    @Parameter(names = {"-u", "--seed-uris"}, description = "[starting points of graph crawl (aka seed URIs)]", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.SEED_NODE_IDIGBIO.getIRIString());
        add(Seeds.SEED_NODE_GBIF.getIRIString());
        add(Seeds.SEED_NODE_BIOCASE.getIRIString());
    }};

    @Parameter(names = {"-l", "--log",}, description = "select how to show the biodiversity graph", converter = LoggerConverter.class)
    private Logger logMode = Logger.tsv;

    @Override
    public void run() {
        crawl(getCrawlMode());
    }

    protected void crawl(CrawlMode crawlMode) {

        final IRI crawlActivity = toIRI(UUID.randomUUID());
        final IRI biodiversityGraph = toIRI(UUID.randomUUID());
        final IRI biodiversityArchive = toIRI(UUID.randomUUID());

        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    addAll(createCrawlInfo(crawlActivity, biodiversityGraph, biodiversityArchive));
                    addAll(generateSeeds(crawlActivity));
                }};

        File dataDir = new File("data");
        File tmpDir = new File(dataDir, "tmp");
        Persistence blobPersistence = new FilePersistence(
                tmpDir,
                new File(dataDir, "blob"));
        BlobStore blobStore = new AppendOnlyBlobStore(blobPersistence);

        StatementListener listeners[] = {
                new RegistryReaderIDigBio(blobStore, statementQueue::add),
                new RegistryReaderGBIF(blobStore, statementQueue::add),
                new RegistryReaderBioCASE(blobStore, statementQueue::add),
                getStatementLogger()
        };

        Persistence statementPersistence = new FilePersistence(tmpDir, new File(dataDir, "statement"));
        StatementListener archive = (CrawlMode.replay == crawlMode)
                ? createOfflineArchive(statementPersistence, blobStore, listeners)
                : createOnlineArchive(statementPersistence, blobStore, listeners, crawlMode);

        while (!statementQueue.isEmpty()) {
            archive.on(statementQueue.poll());
        }
    }

    private List<Triple> generateSeeds(IRI crawlActivity) {
        return seedUrls.stream()
                .map(uriString -> {
                    IRI seed = toIRI(uriString);
                    return toStatement(seed, RefNodeConstants.USED_BY, crawlActivity);
                }).collect(Collectors.toList());
    }

    static List<Triple> createCrawlInfo(IRI crawlActivity, IRI biodiversityGraph, IRI biodiversityArchive) {
        // new crawl activity created for each crawl

        IRI biodiversityGraphCollection = GRAPH_COLLECTION_IRI;

        IRI biodiversityArchiveCollection = ARCHIVE_COLLECTION_IRI;

        IRI crawler = RefNodeConstants.PRESTON;

        return Arrays.asList(
                toStatement(crawler, IS_A, SOFTWARE_AGENT),
                toStatement(crawler, IS_A, AGENT),
                toStatement(crawler, DESCRIPTION, toEnglishLiteral("Preston is a software program that finds, archives and provides access to biodiversity datasets.")),

                toStatement(Seeds.SEED_NODE_GBIF, IS_A, ORGANIZATION),
                toStatement(RegistryReaderGBIF.GBIF_DATASET_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a registry of Darwin Core archives, and EML descriptors.")),
                toStatement(RegistryReaderGBIF.GBIF_DATASET_REGISTRY, CREATED_BY, Seeds.SEED_NODE_GBIF),

                toStatement(Seeds.SEED_NODE_IDIGBIO, IS_A, ORGANIZATION),
                toStatement(Seeds.SEED_NODE_IDIGBIO, DESCRIPTION, toEnglishLiteral("Provides a registry of Darwin Core archives, and EML descriptors. ")),

                toStatement(RegistryReaderIDigBio.PUBLISHERS, DESCRIPTION, toEnglishLiteral("Provides a registry of RSS Feeds that point to publishers of Darwin Core archives, and EML descriptors.")),
                toStatement(RegistryReaderIDigBio.PUBLISHERS, CREATED_BY, Seeds.SEED_NODE_IDIGBIO),


                toStatement(Seeds.SEED_NODE_BIOCASE, IS_A, ORGANIZATION),
                toStatement(Seeds.SEED_NODE_BIOCASE, DESCRIPTION, toEnglishLiteral("Provides a registry of ABCD archives, Darwin Core archives and EML files.")),

                toStatement(RegistryReaderBioCASE.REF_NODE_REGISTRY, DESCRIPTION, toEnglishLiteral("Provides a registry of RSS Feeds that point to publishers of Darwin Core archives, and EML descriptors.")),
                toStatement(RegistryReaderBioCASE.REF_NODE_REGISTRY, CREATED_BY, Seeds.SEED_NODE_BIOCASE),


                toStatement(crawlActivity, IS_A, ACTIVITY),
                toStatement(crawlActivity, DESCRIPTION, toEnglishLiteral("A crawl event is an activity that discovers biodiversity archives.")),
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#startedAtTime"), RefNodeFactory.nowLiteral()),
                toStatement(crawlActivity, toIRI("http://www.w3.org/ns/prov#endedAtTime"), RefNodeFactory.nowLiteral()),
                toStatement(crawlActivity, WAS_ASSOCIATED_WITH, crawler),

                toStatement(biodiversityGraphCollection, IS_A, ENTITY),
                toStatement(biodiversityGraphCollection, IS_A, COLLECTION),
                toStatement(biodiversityGraphCollection, DESCRIPTION, toEnglishLiteral("A collection of biodiversity graphs.")),
                toStatement(biodiversityGraphCollection, GENERATED_BY, crawlActivity),

                toStatement(biodiversityGraph, IS_A, ENTITY),
                toStatement(biodiversityGraph, IS_A, COLLECTION),
                toStatement(biodiversityGraph, DESCRIPTION, toEnglishLiteral("A version of the biodiversity graph.")),
                toStatement(biodiversityGraph, GENERATED_BY, ACTIVITY),

                toStatement(biodiversityArchiveCollection, IS_A, ENTITY),
                toStatement(biodiversityArchiveCollection, DESCRIPTION, toEnglishLiteral("A collection of biodiversity graph archives.")),
                toStatement(biodiversityArchiveCollection, GENERATED_BY, crawlActivity),
                toStatement(biodiversityArchiveCollection, toIRI("http://www.w3.org/ns/prov#wasDerivedFrom"), biodiversityGraph),

                toStatement(biodiversityArchive, IS_A, ENTITY),
                toStatement(biodiversityArchive, DESCRIPTION, toEnglishLiteral("An nquad archive of the version of this biodiversity graph.")),
                toStatement(biodiversityArchive, GENERATED_BY, crawlActivity),
                toStatement(biodiversityArchive, toIRI("http://www.w3.org/ns/prov#wasDerivedFrom"), biodiversityGraph)


        );
    };

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

    private StatementListener createOfflineArchive(Persistence persistence, BlobStore blobStore, StatementListener listeners[]) {
        StatementStoreImpl statementStore = new StatementStoreImpl(persistence) {
            @Override
            public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
            }
        };
        return new Archiver(blobStore, null, statementStore, listeners);
    }

    private StatementListener createOnlineArchive(Persistence persistence, BlobStore blobStore, StatementListener[] listener, CrawlMode crawlMode) {
        Archiver Archiver = new Archiver(blobStore, Resources::asInputStream, new StatementStoreImpl(persistence), listener);
        Archiver.setResolveOnMissingOnly(CrawlMode.resume == crawlMode);
        return Archiver;
    }

}
