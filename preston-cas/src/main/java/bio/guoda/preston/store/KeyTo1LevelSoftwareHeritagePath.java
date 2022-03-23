package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo1LevelSoftwareHeritagePath implements KeyToPath {

    private final URI baseURI;
    private final HashType type;

    public KeyTo1LevelSoftwareHeritagePath(URI baseURI) {
        this.baseURI = baseURI;
        this.type = HashType.sha256;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String keyStr = key.getIRIString();
        int offset = type.getPrefix().length();

        final String s = baseURI.toString();

        String suffix = keyStr.substring(offset) + "/raw/";
        String path = StringUtils.join(Arrays.asList(s, suffix), "");


        return StringUtils.endsWith(s, ":")
                ? URI.create(path)
                : HashKeyUtil.insertSlashIfNeeded(baseURI, suffix);
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.sha256.equals(HashKeyUtil.hashTypeFor(key));
    }

    private static URI detectPath(URI baseURI) {
        return StringUtils.contains(baseURI.getHost(), "softwareheritage.org")
                && !StringUtils.endsWith(baseURI.getPath(), "sha256:")
                ? URI.create("https://archive.softwareheritage.org/api/1/content/sha256:")
                : baseURI;
    }


}
