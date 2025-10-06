package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class KeyTo1LevelOCIPath implements KeyToPath {

    public static final String PREFIX = "https://ghcr.io/v2/";
    public static final String SUFFIX = "/blobs/sha256:";
    public static final Pattern GITHUB_CONTENT_REPOSITORY = Pattern.compile("(https://){0,1}(ghcr\\.io/)(v2/){0,1}(?<org>[^/]+)/(?<repo>[^/]+).*");
    private final URI remote;

    public KeyTo1LevelOCIPath(URI remote) {
        Matcher matcher = GITHUB_CONTENT_REPOSITORY.matcher(remote.toString());
        if (matcher.matches()) {
            this.remote = URI.create(PREFIX + matcher.group("org") + "/" + matcher.group("repo") + SUFFIX);
        } else {
            this.remote = remote;
        }
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);
        String keyStr = key.getIRIString();
        HashType hashType = HashKeyUtil.hashTypeFor(key);
        int offset = hashType.getPrefix().length();
        return URI.create(remote + keyStr.substring(offset));
    }

    @Override
    public boolean supports(IRI key) {
        return HashType.sha256.equals(HashKeyUtil.hashTypeFor(key));
    }

}
