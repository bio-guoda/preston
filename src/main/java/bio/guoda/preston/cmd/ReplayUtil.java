package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.process.VersionedRDFChainEmitter;
import bio.guoda.preston.store.ArchiverReadOnly;
import bio.guoda.preston.store.StatementStoreReadOnly;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public final class ReplayUtil {

    private static final Log LOG = LogFactory.getLog(ReplayUtil.class);


    static void attemptReplay(final BlobStoreReadOnly provenanceLogStore,
                              final StatementStoreReadOnly provenanceLogIndex,
                              StatementsListener... listeners) {
        attemptReplay(provenanceLogStore, provenanceLogIndex, BIODIVERSITY_DATASET_GRAPH, listeners);
    }

    static void attemptReplay(final BlobStoreReadOnly provenanceLogStore,
                              final StatementStoreReadOnly provenanceLogIndex,
                              final IRI provRoot,
                              StatementsListener... listeners) {
        attemptReplay(provenanceLogStore, provenanceLogIndex, new CmdContext(new ProcessorState() {
            @Override
            public boolean shouldKeepProcessing() {
                return true;
            }
        }, provRoot, listeners));
    }

    static void attemptReplay(final BlobStoreReadOnly provenanceLogStore,
                              final StatementStoreReadOnly provenanceLogIndex,
                              final CmdContext ctx) {

        final Queue<Quad> statementQueue =
                new ConcurrentLinkedQueue<Quad>() {{
                    add(toStatement(ctx.getProvRoot(), HAS_VERSION, toBlank()));
                }};

        AtomicBoolean receivedSomething = new AtomicBoolean(false);
        List<StatementsListener> statementListeners = new ArrayList<>(Arrays.asList(ctx.getListeners()));
        statementListeners.add(new StatementsListenerAdapter() {
            @Override
            public void on(Quad statement) {
                receivedSomething.set(true);
            }
        });

        // lookup previous provenance log versions with the intent to replay
        VersionedRDFChainEmitter provenanceLogEmitter = new VersionedRDFChainEmitter(
                provenanceLogStore,
                ctx.getState(),
                statementListeners.toArray(new StatementListener[0])
        );

        StatementListener offlineArchive = new ArchiverReadOnly(
                provenanceLogIndex,
                provenanceLogEmitter);

        while (ctx.getState().shouldKeepProcessing() && !statementQueue.isEmpty()) {
            offlineArchive.on(statementQueue.poll());
        }

        if (!receivedSomething.get()) {
            LOG.warn("No previous updates found. Please update first.");
        }
    }

}
