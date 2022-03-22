package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.SketchIntersectBloomFilter;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.SketchIntersectTheta;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;
import java.util.Arrays;
import java.util.List;

@Parameters(separators = "= ", commandDescription = "uses pre-calculated sketches (e.g., bloom filter, theta sketch) to calculates estimates for overlap between datasets")
public class CmdSketchDiff extends LoggingPersisting implements Runnable {

    private InputStream inputStream = System.in;

    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())));
        run(blobStoreAppendOnly);
    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                System.out, () -> System.exit(0));


        List<ProcessorReadOnly> diffCalculators = Arrays.asList(
                new SketchIntersectBloomFilter(blobStoreReadOnly, listener),
                new SketchIntersectTheta(blobStoreReadOnly, listener));

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                diffCalculators.forEach(x -> x.on(statement));
            }
        };

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(inputStream);

    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

}

