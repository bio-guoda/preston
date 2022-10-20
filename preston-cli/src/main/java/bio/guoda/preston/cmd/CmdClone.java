package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.jena.ext.com.google.common.collect.Streams;
import picocli.CommandLine;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;

@CommandLine.Command(
        name = "clone",
        aliases = {"pull"},
        description = "Clone biodiversity dataset graph"
)

public class CmdClone extends LoggingPersisting implements Runnable {

    @CommandLine.Parameters(description = "Remote repositories (e.g., https://deeplinker.bio/,https://example.org)")
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
                new ValidatingKeyValueStreamContentAddressedFactory(getHashType())
        );

        CloneUtil.clone(
                keyValueStore,
                keyValueStore,
                getHashType(),
                getTracerOfDescendants(),
                getProvenanceRoot()
        );
    }

}
