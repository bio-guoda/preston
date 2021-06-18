package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo5LevelPath implements KeyToPath {

    private final URI baseURI;

    public KeyTo5LevelPath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();
        int offset = Hasher.getHashPrefix().length();
        String u0 = keyStr.substring(offset + 0, offset + 2);
        String u1 = keyStr.substring(offset + 2, offset + 4);
        String u2 = keyStr.substring(offset + 4, offset + 6);
        String suffix = StringUtils.join(Arrays.asList(u0, u1, u2, keyStr.substring(offset), "data"), "/");
        return HashKeyUtil.insertSlashIfNeeded(baseURI, suffix);
    }
}
