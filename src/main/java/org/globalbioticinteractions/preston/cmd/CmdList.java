package org.globalbioticinteractions.preston.cmd;

import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.StatementLogFactory;
import org.globalbioticinteractions.preston.process.StatementArchiveProcessor;
import org.globalbioticinteractions.preston.process.StatementListener;
import org.globalbioticinteractions.preston.store.Archiver;
import org.globalbioticinteractions.preston.store.BlobStore;
import org.globalbioticinteractions.preston.store.Persistence;
import org.globalbioticinteractions.preston.store.StatementStoreImpl;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.globalbioticinteractions.preston.RefNodeConstants.ARCHIVE_COLLECTION_IRI;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = "list biodiversity graph")
public class CmdList extends CmdCrawl {

    private static final Log LOG = LogFactory.getLog(CmdList.class);

    @Override
    public CrawlMode getCrawlMode() {
        return CrawlMode.replay;
    }

    @Override
    protected void run(BlobStore blobStore, Persistence persistence) {
        attemptReplay(blobStore, persistence);
    }

    private void attemptReplay(BlobStore blobStore, Persistence statementPersistence) {
        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    add(toStatement(ARCHIVE_COLLECTION_IRI, HAS_VERSION, toBlank()));
                }};
        // lookup previous archives with the intent to replay
        StatementListener logger = StatementLogFactory.createLogger(getLogMode());

        AtomicBoolean receivedSomething = new AtomicBoolean(false);
        StatementArchiveProcessor reader = new StatementArchiveProcessor(blobStore,
                logger, statement -> receivedSomething.set(true));

        StatementListener offlineArchive
                = createOfflineArchive(statementPersistence, blobStore, reader);

        while (!statementQueue.isEmpty()) {
            offlineArchive.on(statementQueue.poll());
        }

        if (!receivedSomething.get()) {
            LOG.warn("No previous updates found. Please update first.");
        }
    }

    private StatementListener createOfflineArchive(Persistence persistence, BlobStore blobStore, StatementListener... listeners) {
        StatementStoreImpl statementStore = new StatementStoreImpl(persistence) {
            @Override
            public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
            }
        };
        return new Archiver(blobStore, null, statementStore, null, listeners);
    }

}
