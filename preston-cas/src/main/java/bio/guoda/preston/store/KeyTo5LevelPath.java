package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo5LevelPath extends KeyToPathAcceptsAnyValid {

    private final URI remote;

    public KeyTo5LevelPath(URI remote) {
        this.remote = remote;
    }

    @Override
    public URI toPath(IRI key) {
        HashType type = HashKeyUtil.getHashTypeOrThrow(key);

        String keyStr = key.getIRIString();
        int offset = type.getPrefix().length();
        String u0 = keyStr.substring(offset + 0, offset + 2);
        String u1 = keyStr.substring(offset + 2, offset + 4);
        String u2 = keyStr.substring(offset + 4, offset + 6);
        String suffix = StringUtils.join(Arrays.asList(u0, u1, u2, keyStr.substring(offset), "data"), "/");
        return HashKeyUtil.insertSlashIfNeeded(remote, suffix);
    }
}
