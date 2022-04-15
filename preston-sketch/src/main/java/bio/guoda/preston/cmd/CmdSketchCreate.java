package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.SketchBuilder;
import bio.guoda.preston.process.SketchBuilderBloomFilter;
import bio.guoda.preston.process.SketchBuilderTheta;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;

@Parameters(separators = "= ", commandDescription = CmdSketchCreate.CREATES_SKETCHES_E_G_BLOOM_FILTERS_OR_THETA_SKETCHES_FROM_MATCHED_CONTENT_FOR_ESTIMATING_CONTENT_OVERLAP)

@CommandLine.Command(
        name = "sketch",
        aliases = {"bloom"},
        description = CmdSketchCreate.CREATES_SKETCHES_E_G_BLOOM_FILTERS_OR_THETA_SKETCHES_FROM_MATCHED_CONTENT_FOR_ESTIMATING_CONTENT_OVERLAP
)

public class CmdSketchCreate extends LoggingPersisting implements Runnable {

    public static final String CREATES_SKETCHES_E_G_BLOOM_FILTERS_OR_THETA_SKETCHES_FROM_MATCHED_CONTENT_FOR_ESTIMATING_CONTENT_OVERLAP = "Creates sketches (e.g., bloom filters or theta sketches) from matched content for estimating content overlap";
    public static final String SKETCH_TYPE = "Sketch type";

    @Parameter(names = {"-s", "--sketch-type",}, description = SKETCH_TYPE, converter = SketchTypeConverter.class)
    @CommandLine.Option(names = {"-s", "--sketch-type"}, description = SKETCH_TYPE)
    private SketchType sketch = SketchType.bloom;

    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(blobStoreAppendOnly);
    }

    public void run(BlobStore blobStore) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR
        );

        try (SketchBuilder sketchCreator = getSketchCreator(blobStore, listener)) {
            StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

                @Override
                public void emit(Quad statement) {
                    sketchCreator.on(statement);
                }
            };

            new EmittingStreamRDF(emitter, this)
                    .parseAndEmit(getInputStream());
        } catch (IOException e) {
            throw new RuntimeException("failed to generate bloom filter", e);
        }

    }

    private SketchBuilder getSketchCreator(BlobStore blobStore, StatementsListener listener) {
        return sketch.equals(SketchType.theta)
                ? new SketchBuilderTheta(blobStore, listener)
                : new SketchBuilderBloomFilter(blobStore, listener);
    }

}

