package bio.guoda.preston.cmd;

import bio.guoda.preston.store.AppendOnlyBlobStore;
import bio.guoda.preston.store.Persistence;
import bio.guoda.preston.store.StatementStoreImpl;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.VersionLogger;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.Archiver;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;

import java.io.IOException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

@Parameters(separators = "= ", commandDescription = "list biodiversity dataset graph")
public class CmdList extends LoggingPersisting implements Runnable {

    private static final Log LOG = LogFactory.getLog(CmdList.class);

    @Override
    public void run() {
        BlobStore blobStore = new AppendOnlyBlobStore(getBlobPersistence());

        StatementStore statementPersistence = new StatementStoreImpl(getStatementPersistence());
        attemptReplay(blobStore, statementPersistence);
    }

    private void attemptReplay(final BlobStore blobStore, final StatementStore statementPersistence) {
        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    add(toStatement(ARCHIVE, HAS_VERSION, toBlank()));
                }};

        // lookup previous archives with the intent to replay
        StatementListener logger = StatementLogFactory.createLogger(getLogMode());

        AtomicBoolean receivedSomething = new AtomicBoolean(false);
        VersionLogger reader = new VersionLogger(
                blobStore, logger, statement -> receivedSomething.set(true));

        StatementListener offlineArchive = new Archiver(blobStore, null, new StatementStore() {
            @Override
            public void put(Pair<RDFTerm, RDFTerm> queryKey, RDFTerm value) throws IOException {
            }

            @Override
            public IRI get(Pair<RDFTerm, RDFTerm> queryKey) throws IOException {
                return statementPersistence.get(queryKey);
            }
        }, null, reader);

        while (!statementQueue.isEmpty()) {
            offlineArchive.on(statementQueue.poll());
        }

        if (!receivedSomething.get()) {
            LOG.warn("No previous updates found. Please update first.");
        }
    }

}
