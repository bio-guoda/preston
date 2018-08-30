package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Resources;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.process.ContentResolver;
import org.globalbioticinteractions.preston.process.RefStatementListener;
import org.globalbioticinteractions.preston.process.RegistryReaderGBIF;
import org.globalbioticinteractions.preston.process.RegistryReaderIDigBio;
import org.globalbioticinteractions.preston.process.StatementHashLogger;
import org.globalbioticinteractions.preston.process.StatementLogger;
import org.globalbioticinteractions.preston.store.AppendOnlyBlobStore;
import org.globalbioticinteractions.preston.store.AppendOnlyStatementStore;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.FilePersistence;
import org.globalbioticinteractions.preston.store.Persistence;
import org.globalbioticinteractions.preston.store.StatementStore;

import java.beans.Statement;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Parameters(separators = "= ", commandDescription = "show biodiversity graph")
public class CmdList implements Runnable {

    @Parameter(names = {"-u", "--seed-uris"}, description = "[starting points of graph crawl (aka seed URIs)]", validateWith = URIValidator.class)
    private List<String> seedUrls = new ArrayList<String>() {{
        add(Seeds.SEED_NODE_GBIF.getLabel());
        add(Seeds.SEED_NODE_IDIGBIO.getLabel());
    }};

    @Parameter(names = {"-c", "--crawl", }, description = "select how to crawl the biodiversity graph", converter = CrawlModeConverter.class)
    private CrawlMode crawlMode = CrawlMode.full;

    @Parameter(names = {"-l", "--log", }, description = "select how to show the biodiversity graph", converter = LoggerConverter.class)
    private Logger logMode = Logger.tsv;

    @Override
    public void run() {
        final List<RefStatement> seeds = seedUrls.stream()
                .map(uriString -> {
                    RefNode refNodeRoot = RefNodeConstants.SEED_ROOT;
                    RefNode refNodeSeed = new RefNodeString(uriString);
                    return new RefStatement(refNodeRoot, RefNodeConstants.SEED_OF, refNodeSeed);
                }).collect(Collectors.toList());

        final Queue<RefStatement> statements =
                new ConcurrentLinkedQueue<RefStatement>() {{
                    addAll(seeds);
                }};

        Persistence persistence = new FilePersistence();
        BlobStore blobStore = new AppendOnlyBlobStore(persistence);

        StatementStore<URI> statementStore = CrawlMode.replay == crawlMode
                ? createOfflineStatementStore(persistence, blobStore)
                : createOnlineStatementStore(persistence, blobStore);

        final RefStatementListener listener = new ContentResolver(blobStore, statementStore,
                new RegistryReaderIDigBio(statements::add),
                new RegistryReaderGBIF(statements::add),
                getStatementLogger());

        while (!statements.isEmpty()) {
            listener.on(statements.poll());
        }

    }

    private RefStatementListener getStatementLogger() {
        RefStatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLogger();
        } else if (Logger.nquads == logMode) {
            logger = new StatementHashLogger();
        } else {
            logger = new RefStatementListener() {
                AtomicLong count = new AtomicLong(1);
                @Override
                public void on(RefStatement statement) {
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

    private StatementStore<URI> createOfflineStatementStore(Persistence persistence, BlobStore blobStore) {
        return new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream) {

            @Override
            public void put(Pair<URI, URI> partialStatement, URI value) throws IOException {

            }

            @Override
            public void put(Triple<URI, URI, URI> statement) throws IOException {

            }

        };
    }

   private StatementStore<URI> createOnlineStatementStore(Persistence persistence, BlobStore blobStore) {
       AppendOnlyStatementStore appendOnlyStatementStore = new AppendOnlyStatementStore(blobStore, persistence, Resources::asInputStream);
       appendOnlyStatementStore.setResolveOnMissingOnly(CrawlMode.resume == crawlMode);
       return appendOnlyStatementStore;
    }

}
