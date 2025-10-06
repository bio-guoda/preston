package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyTo3LevelTarGzPathShort extends KeyToPathAcceptsAnyValid {

    private final URI remote;
    private final HashType type;


    public KeyTo3LevelTarGzPathShort(URI remote, HashType type) {
        this.remote = remote;
        this.type = type;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();

        int offset = type.getPrefix().length();
        String first = keyStr.substring(offset + 0, offset + 1);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);
        URI uri = HashKeyUtil.insertSlashIfNeeded(remote, "preston-" + first + ".tar.gz!/" + suffix);
        return URI.create("tgz:" + uri.toString());
    }

}
