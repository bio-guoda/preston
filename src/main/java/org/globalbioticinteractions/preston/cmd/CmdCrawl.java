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
import org.globalbioticinteractions.preston.process.RefStatementListener;
import org.globalbioticinteractions.preston.process.RegistryReaderBioCASE;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.StatementLogger;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyStatementStore;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.Persistence;

import java.io.File;
import java.io.IOException;
import java.net.URI;
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

    @Parameter(names = {"-l", "--log",}, description = "select how toLiteral show the biodiversity graph", converter = LoggerConverter.class)
    private Logger logMode = Logger.tsv;

    @Override
    public void run() {
        crawl(getCrawlMode());
    }

    protected void crawl(CrawlMode crawlMode) {
        final List<Triple> seeds = seedUrls.stream()
                .map(uriString -> {
                    IRI refNodeSeed = RefNodeFactory.toIRI(uriString);
                    return RefNodeFactory.toStatement(refNodeSeed, RefNodeConstants.SEED_OF, RefNodeConstants.SOFTWARE_AGENT);
                }).collect(Collectors.toList());

        final Queue<Triple> statements =
                new ConcurrentLinkedQueue<Triple>() {{
                    addAll(seeds);
                }};

        File dataDir = new File("data");
        File tmpDir = new File(dataDir, "tmp");
        Persistence blobPersistence = new FilePersistence(
                tmpDir,
                new File(dataDir, "blob"));
        BlobStore blobStore = new AppendOnlyBlobStore(blobPersistence);

        RefStatementListener listeners[] = {
                new RegistryReaderIDigBio(blobStore, statements::add),
                new RegistryReaderGBIF(blobStore, statements::add),
                new RegistryReaderBioCASE(blobStore, statements::add),
                getStatementLogger()
        };

        Persistence statementPersistence = new FilePersistence(tmpDir, new File(dataDir, "statement"));
        RefStatementListener statementStore = (CrawlMode.replay == crawlMode)
                ? createOfflineStatementStore(statementPersistence, blobStore, listeners)
                : createOnlineStatementStore(statementPersistence, blobStore, listeners, crawlMode);

        while (!statements.isEmpty()) {
            statementStore.on(statements.poll());
        }
    }

    private RefStatementListener getStatementLogger() {
        RefStatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLogger();
        } else {
            logger = new RefStatementListener() {
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

    private RefStatementListener createOfflineStatementStore(Persistence persistence, BlobStore blobStore, RefStatementListener listeners[]) {
        return new AppendOnlyStatementStore(blobStore, persistence, null, listeners) {

            @Override
            public void put(Pair<RDFTerm, RDFTerm> partialStatement, RDFTerm value) throws IOException {

            }


        };
    }

    private RefStatementListener createOnlineStatementStore(Persistence persistence, BlobStore blobStore, RefStatementListener[] listener, CrawlMode crawlMode) {
        AppendOnlyStatementStore appendOnlyStatementStore = new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream, listener);
        appendOnlyStatementStore.setResolveOnMissingOnly(CrawlMode.resume == crawlMode);
        return appendOnlyStatementStore;
    }

}
