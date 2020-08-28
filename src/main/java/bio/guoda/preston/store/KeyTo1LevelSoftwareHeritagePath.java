package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
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
        int offset = Hasher.getHashPrefix().length();

        final String s = baseURI.toString();

        String path = StringUtils.join(Arrays.asList(s, keyStr.substring(offset), "/raw/"), "");
        return URI.create(path);
    }

}
