package bio.guoda.preston.cmd;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.URIConverter;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.ext.com.google.common.collect.Streams;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.cmd.ReplayUtil.attemptReplay;

@Parameters(separators = "= ", commandDescription = "Clone biodiversity dataset graph")
public class CmdClone extends LoggingPersisting implements Runnable {

    @Parameter(description = "remote repositories (e.g., https://deeplinker.bio/,https://example.org)", converter = URIConverter.class, validateWith = URIValidator.class)
    private List<URI> remotes;

    @Override
    protected List<URI> getRemoteURIs() {
        Stream<URI> remotesOrEmpty = remotes == null ? Stream.empty() : remotes.stream();
        Stream<URI> additionalRemotesOrEmpty = super.getRemoteURIs() == null ? Stream.empty() : super.getRemoteURIs().stream();
        return Streams
                .concat(remotesOrEmpty, additionalRemotesOrEmpty)
                .collect(Collectors.toList());
    }

    @Override
    public void run() {
        CloneUtil.clone(getKeyValueStore());
    }

}
