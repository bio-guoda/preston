package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo1LevelSoftwareHeritagePath implements KeyToPath {

    private final URI baseURI;

    public KeyTo1LevelSoftwareHeritagePath(URI baseURI) {
        this.baseURI = baseURI;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();
        int offset = HashType.sha256.getPrefix().length();

        final String s = baseURI.toString();

        String suffix = keyStr.substring(offset) + "/raw/";
        String path = StringUtils.join(Arrays.asList(s, suffix), "");


        return StringUtils.endsWith(s, ":")
                ? URI.create(path)
                : HashKeyUtil.insertSlashIfNeeded(baseURI, suffix);
    }

}
