package bio.guoda.preston.cmd;

import bio.guoda.preston.store.KeyValueStore;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.URIConverter;
import org.apache.jena.ext.com.google.common.collect.Streams;

import java.net.URI;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        KeyValueStore keyValueStore = getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory());
        CloneUtil.clone(keyValueStore, keyValueStore, getKeyValueStore(new KeyValueStoreLocalFileSystem.KeyValueStreamFactorySHA256Values()));
    }

}
