package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.io.output.NullPrintStream;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
        name = "ris-stream",
        description = "Stream RIS records into line-json with Zenodo metadata"
)
public class CmdZoteroStream extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"--community", "--communities"},
            split = ",",
            description = "select which Zenodo communities to submit to. If community is known (e.g., batlit, taxodros), default metadata is included."
    )

    private List<String> communities = new ArrayList<>();


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.INSTANCE,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        StatementsListener textMatcher = new ZoteroFileExtractor(
                this,
                blobStoreReadOnly,
                getOutputStream(),
                communities,
                listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamOfAnyVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

