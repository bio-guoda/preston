package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.ProcessorReadOnly;
import bio.guoda.preston.process.SketchIntersectBloomFilter;
import bio.guoda.preston.process.SketchIntersectTheta;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.Arrays;
import java.util.List;

@Parameters(separators = "= ", commandDescription = CmdSketchDiff.USES_PRE_CALCULATED_SKETCHES_E_G_BLOOM_FILTER_THETA_SKETCH_TO_CALCULATES_ESTIMATES_FOR_OVERLAP_BETWEEN_DATASETS)

@CommandLine.Command(
        name = "diff",
        aliases = {"intersect"},
        description = CmdSketchDiff.USES_PRE_CALCULATED_SKETCHES_E_G_BLOOM_FILTER_THETA_SKETCH_TO_CALCULATES_ESTIMATES_FOR_OVERLAP_BETWEEN_DATASETS
)

public class CmdSketchDiff extends LoggingPersisting implements Runnable {

    public static final String USES_PRE_CALCULATED_SKETCHES_E_G_BLOOM_FILTER_THETA_SKETCH_TO_CALCULATES_ESTIMATES_FOR_OVERLAP_BETWEEN_DATASETS = "Uses pre-calculated sketches (e.g., bloom filter, theta sketch) to calculates estimates for overlap between datasets";

    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(blobStoreAppendOnly);
    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);


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
                .parseAndEmit(getInputStream());

    }

}

