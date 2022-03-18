package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo3LevelTarGzPath implements KeyToPath {

    private final URI baseURI;

    public KeyTo3LevelTarGzPath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();

        int offset = HashType.sha256.getPrefix().length();
        String u0 = keyStr.substring(offset + 0, offset + 2);
        String u1 = keyStr.substring(offset + 2, offset + 4);

        String suffix = StringUtils.join(Arrays.asList(u0, u1, keyStr.substring(offset)), "/");
        URI uri = HashKeyUtil.insertSlashIfNeeded(baseURI, "preston-" + u0 + ".tar.gz!/" + suffix);
        return URI.create("tgz:" + uri.toString());
    }

}
