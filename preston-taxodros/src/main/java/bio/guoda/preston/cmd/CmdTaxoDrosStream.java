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
import java.util.List;
import java.util.Properties;

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

    @CommandLine.Option(
            names = {"--pub-doi"},
            description = "associated Taxodros DOI"
    )
    private String doi = TaxoDrosFileStreamHandler.TAXODROS_DATA_DOI;

    @CommandLine.Option(
            names = {"--pub-md5"},
            description = "associated Taxodros md5 fingerprint"
    )
    private String md5 = TaxoDrosFileStreamHandler.TAXODROS_DATA_VERSION_MD5;

    @CommandLine.Option(
            names = {"--pub-sha256"},
            description = "associated Taxodros sha256 fingerprint"
    )
    private String sha256 = TaxoDrosFileStreamHandler.TAXODROS_DATA_VERSION_SHA256;

    @CommandLine.Option(
            names = {"--pub-year"},
            description = "associated Taxodros publication year"
    )
    private String year = TaxoDrosFileStreamHandler.TAXODROS_DATA_YEAR;


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        run(BlobStoreUtil.createResolvingBlobStoreFor(blobStoreAppendOnly, this));

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
                new Properties() {{
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_DOI, doi);
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_VERSION_SHA256, sha256);
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_VERSION_MD5, md5);
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_YEAR, year);
                }},
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

