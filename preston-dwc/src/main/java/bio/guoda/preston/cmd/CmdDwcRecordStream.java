package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

@CommandLine.Command(
        name = "dwc-stream",
        aliases = {"json-stream", "dwc-json-stream"},
        description = CmdDwcRecordStream.EXTRACT_RECORDS_FROM_DARWIN_CORE_ARCHIVES_IN_LINE_JSON
)
public class CmdDwcRecordStream extends LoggingPersisting implements Runnable {

    public static final String EXTRACT_RECORDS_FROM_DARWIN_CORE_ARCHIVES_IN_LINE_JSON = "Extract records from DarwinCore archives in line-json";

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.NULL_PRINT_STREAM,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        DwcRecordExtractor textMatcher = new DwcRecordExtractor(
                this,
                blobStoreReadOnly,
                getOutputStream(),
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

