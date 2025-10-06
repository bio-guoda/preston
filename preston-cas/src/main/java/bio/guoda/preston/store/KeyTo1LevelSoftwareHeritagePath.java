package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo1LevelSoftwareHeritagePath implements KeyToPath {

    private final URI remote;

    public KeyTo1LevelSoftwareHeritagePath(URI remote) {
        this.remote = remote;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();

        final String s = remote.toString();

        String suffix = keyStr.substring(offset) + "/raw/";
        String path = StringUtils.join(Arrays.asList(s, hashType.name(), ":", suffix), "");


        return StringUtils.equals(s, KeyTo1LevelSoftwareHeritageAutoDetectPath.SOFTWARE_HERITAGE_API_ENDPOINT)
                ? URI.create(path)
                : HashKeyUtil.insertSlashIfNeeded(remote, suffix);
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.sha256.equals(HashKeyUtil.hashTypeFor(key));
    }

}
