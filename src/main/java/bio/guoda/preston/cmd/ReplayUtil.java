package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.VersionLogger;
import bio.guoda.preston.store.ArchiverReadOnly;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementStore;
import bio.guoda.preston.store.StatementStoreReadOnly;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.ARCHIVE;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public final class ReplayUtil {

    private static final Log LOG = LogFactory.getLog(ReplayUtil.class);


    public static void attemptReplay(final BlobStore blobStore,
                                     final StatementStoreReadOnly statementStore,
                                     StatementListener... listeners) {
        attemptReplay(blobStore, statementStore, ARCHIVE, listeners);
    }

    public static void attemptReplay(final BlobStore blobStore,
                                     final StatementStoreReadOnly statementStore,
                                     final IRI provRoot,
                                     StatementListener... listeners) {

        final Queue<Triple> statementQueue =
                new ConcurrentLinkedQueue<Triple>() {{
                    add(toStatement(provRoot, HAS_VERSION, toBlank()));
                }};

        AtomicBoolean receivedSomething = new AtomicBoolean(false);
        List<StatementListener> statementListeners = new ArrayList<>(Arrays.asList(listeners));
        statementListeners.add(statement -> receivedSomething.set(true));

        // lookup previous archives with the intent to replay
        VersionLogger reader = new VersionLogger(
                blobStore,
                statementListeners.toArray(new StatementListener[0])
        );

        StatementListener offlineArchive = new ArchiverReadOnly(
                statementStore,
                reader);

        while (!statementQueue.isEmpty()) {
            offlineArchive.on(statementQueue.poll());
        }

        if (!receivedSomething.get()) {
            LOG.warn("No previous updates found. Please update first.");
        }
    }

    public static void checkAndHandle(final PrintStream out, LogErrorHandler handler) {
        if (out.checkError()) {
            handler.handleError();
        }
    }
}
