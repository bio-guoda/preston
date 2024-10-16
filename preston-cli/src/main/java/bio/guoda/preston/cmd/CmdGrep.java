package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TextMatcher;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.regex.Pattern;

@CommandLine.Command(
        name = "grep",
        aliases = {"match", "findURLs"},
        description = "Uses Java-style regular expression (e.g., \"Homo sapie.*\") to find matching content in biodiversity dataset graph"
)
public class CmdGrep extends LoggingPersisting implements Runnable {

    @CommandLine.Parameters(description = "regular expression (e.g., \"Homo sapien.*\")")
    private String regex = TextMatcher.URL_PATTERN.pattern();

    @CommandLine.Option(
            names = {"--max", "--max-per-content"},
            description = "Maximum number of matched texts to record for each content; set to 0 for no limit"
    )
    private int maxHitsPerContent = 0;

    @CommandLine.Option(
            names = {"-o", "--only-matching"},
            description = "Report only the text that was matched"
    )
    private boolean reportOnlyMatchingText = false;

    @CommandLine.Option(
            names = {"--no-line", "--no-lines"},
            description = "Don't report line numbers for matches"
    )
    private boolean dontSeparateLines = false;

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()), true, getHashType());
        run(BlobStoreUtil.createResolvingBlobStoreFor(blobStoreAppendOnly, this));

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                getOutputStream(),
                LogErrorHandlerExitOnError.EXIT_ON_ERROR
        );

        TextMatcher textMatcher = new TextMatcher(
                Pattern.compile(regex),
                maxHitsPerContent,
                reportOnlyMatchingText,
                !dontSeparateLines,
                this,
                blobStoreReadOnly,
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

