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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

@CommandLine.Command(
        name = "taxodros-stream",
        description = "Stream TaxoDros https://www.taxodros.uzh.ch/ records into line-json with Zenodo metadata; Also see Bächli, G. (2024). TaxoDros - The Database on Taxonomy of Drosophilidae hash://md5/d68c923002c43271cee07ba172c67b0b hash://sha256/3e41eec4c91598b8a2de96e1d1ed47d271a7560eb6ef350a17bc67cc61255302 [Data set]. Zenodo. https://doi.org/10.5281/zenodo.10565403 ."
)
public class CmdTaxoDrosStream extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"--communities"},
            description = "associated Zenodo communities"
    )
    private List<String> communities = Arrays.asList("taxodros", "biosyslit");

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

        TaxoDrosFileExtractor textMatcher = new TaxoDrosFileExtractor(
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

