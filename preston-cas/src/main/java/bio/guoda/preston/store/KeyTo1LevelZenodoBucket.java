package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

public class KeyTo1LevelZenodoBucket implements KeyToPath {

    private final KeyToPath proxied;

    private final AtomicReference<URI> lastZenodoBucket = new AtomicReference<URI>();

    public KeyTo1LevelZenodoBucket(KeyToPath keyToPath) {
        this.proxied = keyToPath;
    }

    @Override
    public URI toPath(IRI key) {
        URI path = null;
        if (HashType.md5.equals(HashKeyUtil.hashTypeFor(key))) {
            URI fileURI = proxied.toPath(key);
            if (fileURI != null) {
                String bucketEndpoint = parseZenodoBucketEndpoint(fileURI);
                lastZenodoBucket.set(URI.create(bucketEndpoint));
            }
            path = fileURI;
        } else {
            URI baseURI = lastZenodoBucket.get();
            if (baseURI != null) {
                path = new KeyTo1LevelPath(baseURI)
                        .toPath(key);
            }
        }
        return path;
    }

    public static String parseZenodoBucketEndpoint(URI fileURI) {
        String bucketEndpoint = null;
        String uriString = fileURI.toString();
        int i = uriString.lastIndexOf("/");
        if (i > 0) {
            bucketEndpoint = uriString.substring(0, i + 1);
        }
        return bucketEndpoint;
    }

    @Override
    public boolean supports(IRI key) {
        return HashKeyUtil.isValidHashKey(key);
    }


}
