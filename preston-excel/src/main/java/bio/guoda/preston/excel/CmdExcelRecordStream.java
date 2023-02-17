package bio.guoda.preston.excel;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.LoggingPersisting;
import bio.guoda.preston.process.EmittingStreamOfVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.IOException;

@CommandLine.Command(
        name = "excel-stream",
        aliases = {"excel2json"},
        description = "Extract records from Excel files (*.xls, *.xlsx) in line-json"
)
public class CmdExcelRecordStream extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"--headerless"},
            description = "use column numbers as column names"
    )
    private Boolean headerless = false;

    @CommandLine.Option(
            names = {"--skip-lines"},
            description = "skip specified number of lines before processing the xlsx worksheet"
    )
    private Integer skipLines = 0;



    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                if (RefNodeFactory.hasVersionAvailable(statement)) {
                    BlankNodeOrIRI version = RefNodeFactory.getVersion(statement);
                    try {
                        readXLSX((IRI) version, skipLines, headerless);
                        readXLS((IRI) version, skipLines, headerless);
                    } catch (IOException e) {
                        // ignore
                    }
                }

            }

            void readXLS(IRI version, Integer skipLines, Boolean headerless) {
                try {
                    XLSHandler.asJsonStream(
                            getOutputStream(),
                            version,
                            blobStoreReadOnly,
                            skipLines,
                            headerless);
                } catch (IOException e) {
                    // ignore
                }
            }

            void readXLSX(IRI version, Integer skipLines, Boolean headerless) throws IOException {
                XLSXHandler.asJsonStream(
                        getOutputStream(),
                        version,
                        blobStoreReadOnly,
                        skipLines,
                        headerless);
            }
        };

        new EmittingStreamOfVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

