package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.InputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeyTo1LevelOCIPath implements KeyToPath {

    public static final String PREFIX = "https://ghcr.io/v2/";
    public static final String SUFFIX = "/blobs/sha256:";
    public static final Pattern GITHUB_CONTENT_REPOSITORY = Pattern.compile("(https://){0,1}(ghcr\\.io/)(v2/){0,1}(?<org>[^/]+)/(?<repo>[^/]+).*");
    private final URI baseURI;

    public KeyTo1LevelOCIPath(URI baseURI) {
        Matcher matcher = GITHUB_CONTENT_REPOSITORY.matcher(baseURI.toString());
        if (matcher.matches()) {
            this.baseURI = URI.create(PREFIX + matcher.group("org") + "/" + matcher.group("repo") + SUFFIX);
        } else {
            this.baseURI = baseURI;
        }
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);
        String keyStr = key.getIRIString();
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();
        return URI.create(baseURI + keyStr.substring(offset));
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.sha256.equals(HashKeyUtil.hashTypeFor(key));
    }

}
