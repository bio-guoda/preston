package bio.guoda.preston.zenodo;

import bio.guoda.preston.EnvUtil;
import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.cmd.AnchorUtil;
import bio.guoda.preston.cmd.BlobStoreUtil;
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

import java.util.Collections;
import java.util.List;

@CommandLine.Command(
        name = "zenodo",
        description = "create/update associated Zenodo records"
)
public class CmdZenodo extends LoggingPersisting implements Runnable {

    public static final String ZENODO_ENDPOINT = "ZENODO_ENDPOINT";
    public static final String ZENODO_TOKEN = "ZENODO_TOKEN";

    private String accessToken = EnvUtil.getEnvironmentVariable(ZENODO_TOKEN);

    @CommandLine.Option(
            names = {"--endpoint"},
            description = "Zenodo api endpoint. Uses [ " + ZENODO_ENDPOINT + "] environment variable by default."
    )
    private String apiEndpoint = EnvUtil.getEnvironmentVariable(ZENODO_ENDPOINT, "https://zenodo.org");

    @CommandLine.Option(
            names = {"--skip-on-existing"},
            description = "skip submission of a Zenodo deposit with matching identifiers already exists"
    )
    private Boolean skipOnExisting = true;

    @CommandLine.Option(
            names = {"--communities", "--community"},
            description = "associated Zenodo communities"
    )
    private List<String> communities = Collections.emptyList();


    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                true, getHashType());
        run(BlobStoreUtil.createResolvingBlobStoreFor(blobStoreAppendOnly, this));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR);

        ZenodoContext zenodoContext = new ZenodoContext(
                getAccessToken(),
                getApiEndpoint(),
                getCommunities()
        );
        zenodoContext.setSkipOnExisting(skipOnExisting);
        StatementsListener textMatcher = new ZenodoMetadataFileExtractor(
                this,
                blobStoreReadOnly,
                zenodoContext,
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

    public List<String> getCommunities() {
        return communities;
    }
}
