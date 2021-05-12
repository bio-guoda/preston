package bio.guoda.preston.cmd;

import bio.guoda.preston.StatementLogFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TextMatcher;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.rdf.api.Quad;

import java.io.InputStream;
import java.util.regex.Pattern;

@Parameters(separators = "= ", commandDescription = "Searches identified contents for text that matches the provided regular expression")
public class CmdMatch extends LoggingPersisting implements Runnable {

    @Parameter(description = "regular expression",
            validateWith = RegexValidator.class)
    private String regex = TextMatcher.URL_PATTERN.pattern();

    private InputStream inputStream = System.in;


    @Override
    public void run() {
        BlobStoreAppendOnly blobStoreAppendOnly = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        run(blobStoreAppendOnly);

    }

    public void run(BlobStoreReadOnly blobStoreReadOnly) {
        StatementsListener listener = StatementLogFactory.createPrintingLogger(
                getLogMode(),
                System.out, () -> System.exit(0));

        TextMatcher textMatcher = new TextMatcher(Pattern.compile(regex), 0, this, blobStoreReadOnly, listener);

        StatementsEmitterAdapter emitter = new StatementsEmitterAdapter() {

            @Override
            public void emit(Quad statement) {
                textMatcher.on(statement);
            }
        };

        new EmittingStreamRDF(emitter, this)
                .parseAndEmit(inputStream);

    }

    public void setInputStream(InputStream inputStream) {
        this.inputStream = inputStream;
    }

}

