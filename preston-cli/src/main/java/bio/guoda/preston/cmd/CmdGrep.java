package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
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

import java.io.InputStream;
import java.util.regex.Pattern;

@Parameters(separators = "= ", commandDescription = "Searches identified contents for text that matches the provided regular expression")
public class CmdGrep extends LoggingPersisting implements Runnable {

    @Parameter(description = "regular expression",
            validateWith = RegexValidator.class)
    private String regex = TextMatcher.URL_PATTERN.pattern();

    @Parameter(names = {"--max", "--max-per-content"}, description = "maximum number of matched texts to record for each content; set to 0 for no limit", converter = IntegerConverter.class)
    private int maxHitsPerContent = 0;

    @Parameter(names = {"-o", "--only-matching"}, description = "report only the text that was matched")
    private boolean reportOnlyMatchingText = false;

    @Parameter(names = {"--no-line", "--no-lines"}, description = "don't report line numbers for matches")
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
                System.out, () -> System.exit(0));

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

