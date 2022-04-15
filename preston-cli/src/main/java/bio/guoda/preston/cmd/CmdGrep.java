package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TextMatcher;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.IntegerConverter;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.util.regex.Pattern;

@Parameters(separators = "= ", commandDescription = "Searches identified contents for text that matches the provided regular expression")
@CommandLine.Command(
        name = "grep",
        aliases = {"match", "findURLs"},
        description = CmdSketchDiff.USES_PRE_CALCULATED_SKETCHES_E_G_BLOOM_FILTER_THETA_SKETCH_TO_CALCULATES_ESTIMATES_FOR_OVERLAP_BETWEEN_DATASETS
)
public class CmdGrep extends LoggingPersisting implements Runnable {

    public static final String REGULAR_EXPRESSION = "regular expression";
    public static final String MAXIMUM_NUMBER_OF_MATCHED_TEXTS_TO_RECORD_FOR_EACH_CONTENT_SET_TO_0_FOR_NO_LIMIT = "maximum number of matched texts to record for each content; set to 0 for no limit";
    public static final String REPORT_ONLY_THE_TEXT_THAT_WAS_MATCHED = "report only the text that was matched";
    public static final String DON_T_REPORT_LINE_NUMBERS_FOR_MATCHES = "Don't report line numbers for matches";
    @Parameter(description = REGULAR_EXPRESSION,
            validateWith = RegexValidator.class)

    @CommandLine.Parameters(description = REGULAR_EXPRESSION)
    private String regex = TextMatcher.URL_PATTERN.pattern();

    @Parameter(names = {"--max", "--max-per-content"}, description = MAXIMUM_NUMBER_OF_MATCHED_TEXTS_TO_RECORD_FOR_EACH_CONTENT_SET_TO_0_FOR_NO_LIMIT, converter = IntegerConverter.class)
    @CommandLine.Option(
            names = {"--max", "--max-per-content"},
            description = MAXIMUM_NUMBER_OF_MATCHED_TEXTS_TO_RECORD_FOR_EACH_CONTENT_SET_TO_0_FOR_NO_LIMIT
    )
    private int maxHitsPerContent = 0;

    @Parameter(names = {"-o", "--only-matching"}, description = REPORT_ONLY_THE_TEXT_THAT_WAS_MATCHED)
    @CommandLine.Option(
            names = {"-o", "--only-matching"},
            description = REPORT_ONLY_THE_TEXT_THAT_WAS_MATCHED
    )
    private boolean reportOnlyMatchingText = false;

    @Parameter(names = {"--no-line", "--no-lines"}, description = DON_T_REPORT_LINE_NUMBERS_FOR_MATCHES)
    @CommandLine.Option(
            names = {"--no-line", "--no-lines"},
            description = DON_T_REPORT_LINE_NUMBERS_FOR_MATCHES
    )
    private boolean dontSeparateLines = false;

    @Override
    public void run() {
        BlobStoreReadOnly blobStoreAppendOnly
                = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory(getHashType())), true, getHashType());
        run(resolvingBlobStore(blobStoreAppendOnly));

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

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(getInputStream());

    }

}

