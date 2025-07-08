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

import java.util.ArrayList;
import java.util.List;

@CommandLine.Command(
        name = "ris-stream",
        header = "translates bibliographic citations from RIS format into Zenodo metadata in JSON lines format",
        description = "Stream RIS records into line-json with Zenodo metadata",
        footerHeading = "Examples",
        footer = {
                "%n1.",
                "First, generate a RIS record, record.ris:",
                "----",
                "cat > record.ris <<__EOL__",
                "TY  - BOOK",
                "TI  - Faber, Helen R May 5, 1913",
                "T2  - Walter Deane correspondence",
                "UR  - https://www.biodiversitylibrary.org/part/326364",
                "PY  - 1913-05-05",
                "AU  - Faber, Helen R.,",
                "ER  -",
                "__EOL__",
                "----",
                "Then, track record.ris using Preston into Zenodo metadata using: ",
                "----",
                "cat record.ris\\",
                " | preston track",
                "----",
                "Following, append the associated bhl pdf via: ",
                "----",
                "preston track https://www.biodiversitylibrary.org/partpdf/326364",
                "----",
                "Finally, generate Zenodo metadata record.json using: ",
                "----",
                "preston head\\",
                " | preston cat\\",
                " | preston ris-stream\\",
                " > record.json",
                "----"
        }

)
public class CmdRISStream extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"--community", "--communities"},
            split = ",",
            description = "select which Zenodo communities to submit to. If community is known (e.g., batlit, taxodros), default metadata is included."
    )

    private List<String> communities = new ArrayList<>();

    @CommandLine.Option(
            names = {"--reuse-doi"},
            defaultValue = "false",
            description = "use existing DOI in Zenodo deposit if available"
    )
    private Boolean ifAvailableUseExistingDOI = false;


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreReadonly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        BlobStoreReadOnly blobStoreWithIndexedVersions = BlobStoreUtil.createIndexedBlobStoreFor(blobStoreReadonly, this);

        run(blobStoreWithIndexedVersions);

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                NullPrintStream.INSTANCE,
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        StatementsListener textMatcher = new RISFileExtractor(
                this,
                blobStoreReadOnly,
                getOutputStream(),
                communities,
                ifAvailableUseExistingDOI,
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

