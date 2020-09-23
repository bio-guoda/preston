package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.TextMatcher;
import bio.guoda.preston.store.BlobStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;

import java.util.stream.Stream;

@Parameters(separators = "= ", commandDescription = "Searches identified contents for text that matches the provided regular expression")
public class CmdMatch extends CmdProcess {

    @Parameter(description = "regular expression",
            validateWith = RegexValidator.class)
    private String regex = TextMatcher.URL_PATTERN.pattern();

    @Override
    protected Stream<StatementsListener> createProcessors(BlobStore blobStore, StatementsListener queueAsListener) {
        return Stream.of(
                new TextMatcher(regex, blobStore, queueAsListener)
        );
    }

    @Override
    String getActivityDescription() {
        return "An event that finds the locations of text matching the regular expression '" + regex + "' inside any encountered content (e.g., hash://sha256/... identifiers).";
    }
}
