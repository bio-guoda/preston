package bio.guoda.preston.zenodo;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.cmd.LogErrorHandlerExitOnError;
import bio.guoda.preston.cmd.LoggingPersisting;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

@CommandLine.Command(
        name = "zenodo",
        description = "create/update associated Zenodo records"
)
public class CmdZenodo extends LoggingPersisting implements Runnable {

    @CommandLine.Option(
            names = {"--access-token"},
            description = "Zenodo Access Token"
    )
    private String accessToken;

    @CommandLine.Option(
            names = {"--endpoint"},
            description = "Zenodo api endpoint"
    )
    private String apiEndpoint = "https://zenodo.org";


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        ZenodoMetadataFileExtractor textMatcher = new ZenodoMetadataFileExtractor(
                this,
                blobStoreReadOnly,
                new ZenodoContext(getAccessToken(), getApiEndpoint()),
                listener
        );

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };


        new EmittingStreamOfAnyVersions(emitter, this)
                .parseAndEmit(getInputStream());

    }

    private String getAccessToken() {
        return accessToken;
    }

    public String getApiEndpoint() {
        return apiEndpoint;
    }

    public void setApiEndpoint(String apiEndpoint) {
        this.apiEndpoint = apiEndpoint;
    }

    public void setAccessToken(String accessToken) {
        this.accessToken = accessToken;
    }
}