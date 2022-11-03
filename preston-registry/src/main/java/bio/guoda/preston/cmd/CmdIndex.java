package bio.guoda.preston.cmd;

import bio.guoda.preston.ArchiveUtil;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.index.QuadIndexImpl;
import bio.guoda.preston.process.ActivityUtil;
import bio.guoda.preston.process.RDFReader;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementsListenerEmitterAdapter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.*;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class CmdIndex extends CmdAppend {
    @Override
    void initQueue(Queue<List<Quad>> statementQueue, ActivityContext ctx) {
        // Don't queue anything
    }

    @Override
    void processQueue(Queue<List<Quad>> statementQueue,
                      BlobStore blobStore,
                      ActivityContext ctx,
                      StatementsListener[] listeners) {

        IRI generation = toIRI(UUID.randomUUID());
        File indexDir = getTmpDir().toPath().resolve(generation.getIRIString()).toFile();

        StatementsListenerEmitterAdapter queueAsListenerEmitter = new StatementsListenerEmitterAdapter() {
            @Override
            public void on(Quad statement) {
                statementQueue.add(Collections.singletonList(statement));
            }

            @Override
            public void emit(Quad statement) {
                on(statement);
            }
        };
        queueAsListenerEmitter.emit(toStatement(generation, generation, IS_A, GENERATION));

        try {
            try (QuadIndexImpl quadIndex = new QuadIndexImpl(indexDir)) {
                RDFReader provIndexer = new RDFReader(
                        blobStore,
                        queueAsListenerEmitter,
                        generation,
                        (unspecificOrigin) -> new StatementsEmitterAdapter() {
                            @Override
                            public void emit(Quad statement) {
                                quadIndex.put(statement, unspecificOrigin);
                            }
                        },
                        this
                );

                StatementsListener[] listenersWithIndexer = Stream.concat(Arrays.stream(listeners), Stream.of(provIndexer))
                        .toArray(StatementsListener[]::new);

                super.processQueue(statementQueue, blobStore, ctx, listenersWithIndexer);
            }

            packageIndex(generation, ctx, blobStore, indexDir, queueAsListenerEmitter);

        } catch (IOException e) {
            throw new RuntimeException("Indexing failed", e);
        } finally {
            FileUtils.deleteQuietly(indexDir);
        }

        handleQueuedMessages(statementQueue, listeners);
    }

    private void packageIndex(IRI generation, ActivityContext ctx, BlobStore blobStore, File indexDir, StatementsEmitter emitter) {
        try {
            IRI indexVersion = ArchiveUtil.copyDirectoryToBlobstore(blobStore, indexDir);
            Literal nowLiteral = RefNodeFactory.nowDateTimeLiteral();

            ActivityUtil.emitAsNewActivity(
                    Stream.of(
                            toStatement(indexVersion, WAS_GENERATED_BY, generation),
                            toStatement(indexVersion, QUALIFIED_GENERATION, generation),
                            toStatement(generation, GENERATED_AT_TIME, nowLiteral)
                    ),
                    emitter,
                    Optional.of(ctx.getActivity()),
                    generation
            );
        } catch (IOException e) {
            throw new RuntimeException("Failed to package index", e);
        }
    }

    @Override
    String getActivityDescription() {
        return "An event that indexes biodiversity dataset graphs";
    }
}
