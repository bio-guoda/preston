package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.URIConverter;
import org.apache.jena.ext.com.google.common.collect.Streams;
import picocli.CommandLine;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Parameters(separators = "= ", commandDescription = CmdClone.CLONE_BIODIVERSITY_DATASET_GRAPH)

@CommandLine.Command(
        name = "clone",
        aliases = {"pull"},
        description = CmdClone.CLONE_BIODIVERSITY_DATASET_GRAPH
)

public class CmdClone extends LoggingPersisting implements Runnable {

    static final String CLONE_BIODIVERSITY_DATASET_GRAPH = "Clone biodiversity dataset graph";
    private static final String REMOTE_REPOSITORIES_E_G_HTTPS_DEEPLINKER_BIO_HTTPS_EXAMPLE_ORG = "Remote repositories (e.g., https://deeplinker.bio/,https://example.org)";

    @Parameter(description = REMOTE_REPOSITORIES_E_G_HTTPS_DEEPLINKER_BIO_HTTPS_EXAMPLE_ORG, converter = URIConverter.class, validateWith = URIValidator.class)

    @CommandLine.Parameters(description = REMOTE_REPOSITORIES_E_G_HTTPS_DEEPLINKER_BIO_HTTPS_EXAMPLE_ORG)
    private List<URI> remotes;

    @Override
    protected List<URI> getRemotes() {
        Stream<URI> remotesOrEmpty = remotes == null ? Stream.empty() : remotes.stream();
        Stream<URI> additionalRemotesOrEmpty = super.getRemotes() == null ? Stream.empty() : super.getRemotes().stream();
        return Streams
                .concat(remotesOrEmpty, additionalRemotesOrEmpty)
                .collect(Collectors.toList());
    }

    @Override
    public void run() {
        KeyValueStore keyValueStore = getKeyValueStore(
                new KeyValueStoreLocalFileSystem
                        .ValidatingKeyValueStreamContentAddressedFactory(getHashType())
        );

        CloneUtil.clone(
                keyValueStore,
                keyValueStore,
                getHashType(),
                getTracerOfDescendants()
        );
    }

}
