package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyTo3LevelTarGzPathShorter extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;
    private final HashType type;


    public KeyTo3LevelTarGzPathShorter(URI baseURI, HashType type) {
        this.baseURI = baseURI;
        this.type = type;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);
        URI uri = HashKeyUtil.insertSlashIfNeeded(baseURI, "preston.tar.gz!/" + suffix);
        return URI.create("tgz:" + uri.toString());
    }

}
