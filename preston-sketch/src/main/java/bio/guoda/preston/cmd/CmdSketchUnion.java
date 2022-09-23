package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.SketchUnionTheta;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(
        name = "union",
        description =  "Uses pre-calculated sketches (only theta sketches for now) to calculates union of distinct elements found in datasets")
public class CmdSketchUnion extends LoggingPersisting implements Runnable {

    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(blobStoreAppendOnly);
    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);


        try (SketchUnionTheta unionSketch = new SketchUnionTheta(blobStoreReadOnly, listener)) {
            StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    unionSketch.on(statement);
                }
            };

            new EmittingStreamRDF(emitter, this)
                    .parseAndEmit(getInputStream());
        } catch (IOException ex) {
            throw new RuntimeException("failed to calculate union of sketches", ex);
        }

    }

}

