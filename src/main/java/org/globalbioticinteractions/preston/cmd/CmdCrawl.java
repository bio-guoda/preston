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
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.process.RegistryReaderBioCASE;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
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
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

public abstract class CmdCrawl implements Runnable, Crawler {

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

        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    addAll(generateSeeds());
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

    private List<Triple> generateSeeds() {
        return seedUrls.stream()
                .map(uriString -> {
                    IRI refNodeSeed = RefNodeFactory.toIRI(uriString);
                    return RefNodeFactory.toStatement(refNodeSeed, RefNodeConstants.HAD_MEMBER, RefNodeConstants.SOFTWARE_AGENT);
                }).collect(Collectors.toList());
    }

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
