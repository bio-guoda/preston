package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.SketchUnionTheta;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;

@Parameters(separators = "= ", commandDescription = "uses pre-calculated sketches (only theta sketches for now) to calculates union of distinct elements found in datasets")
public class CmdSketchUnion extends LoggingPersisting implements Runnable {

    private InputStream inputStream = System.in;

    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(
                getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())));
        run(blobStoreAppendOnly);
    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                System.out, () -> System.exit(0));


        try (SketchUnionTheta unionSketch = new SketchUnionTheta(blobStoreReadOnly, listener)) {
            StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {
                @Override
                public void emit(Quad statement) {
                    unionSketch.on(statement);
                }
            };

            new EmittingStreamRDF(emitter, this)
                    .parseAndEmit(inputStream);
        } catch (IOException ex) {
            throw new RuntimeException("failed to calculate union of sketches", ex);
        }

    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

}

