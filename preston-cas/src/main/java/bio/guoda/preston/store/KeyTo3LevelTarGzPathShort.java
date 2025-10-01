package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo3LevelTarGzPathShort extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;
    private final HashType type;


    public KeyTo3LevelTarGzPathShort(URI baseURI, HashType type) {
        this.baseURI = baseURI;
        this.type = type;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();

        int offset = type.getPrefix().length();
        String first = keyStr.substring(offset + 0, offset + 1);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);
        URI uri = HashKeyUtil.insertSlashIfNeeded(baseURI, "preston-" + first + ".tar.gz!/" + suffix);
        return URI.create("tgz:" + uri.toString());
    }

}
