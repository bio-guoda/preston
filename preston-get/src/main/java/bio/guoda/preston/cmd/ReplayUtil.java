package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.StatementIRIProcessor;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.process.VersionedRDFChainEmitter;
import bio.guoda.preston.store.ArchiverReadOnly;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.HexaStoreConstants;
import bio.guoda.preston.store.HexaStoreImpl;
import bio.guoda.preston.store.HexaStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toBlank;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public final class ReplayUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayUtil.class);


    static void attemptReplay(final BlobStoreReadOnly provenanceLogStore,
                              final HexaStoreReadOnly provenanceLogIndex,
                              StatementsListener... listeners) {
        attemptReplay(provenanceLogStore, provenanceLogIndex, BIODIVERSITY_DATASET_GRAPH, listeners);
    }

    static void attemptReplay(final BlobStoreReadOnly provenanceLogStore,
                              final HexaStoreReadOnly provenanceLogIndex,
                              final IRI provRoot,
                              StatementsListener... listeners) {
        attemptReplay(provenanceLogStore, provenanceLogIndex, new CmdContext(new ProcessorStateAlwaysContinue(), provRoot, listeners));
    }

    static void attemptReplay(final BlobStoreReadOnly provenanceLogStore,
                              final HexaStoreReadOnly provenanceLogIndex,
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
                statementListeners.toArray(new StatementsListener[0])
        );

        StatementsListener offlineArchive = new ArchiverReadOnly(
                provenanceLogIndex,
                provenanceLogEmitter);

        while (ctx.getState().shouldKeepProcessing() && !statementQueue.isEmpty()) {
            offlineArchive.on(statementQueue.poll());
        }

        if (!receivedSomething.get()) {
            LOG.warn("No provenance found. Please use/create a Preston data archive.");
        }
    }

    public static void replay(StatementsListener listener, Persisting persisting) {
        StatementIRIProcessor processor = new StatementIRIProcessor(listener);
        processor.setIriProcessor(new IRIFixingProcessor());

        attemptReplay(
                new BlobStoreAppendOnly(persisting.getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(persisting.getHashType())), true, persisting.getHashType()),
                new HexaStoreImpl(
                        persisting.getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactoryValues(persisting.getHashType())), persisting.getHashType()
                ),
                new CmdContext(persisting, processor));
    }

}
