package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;
import java.util.Arrays;

public class KeyTo3LevelZipPathExplicit extends KeyToPathAcceptsAnyValid {

    private final URI baseURI;
    private final HashType type;


    public KeyTo3LevelZipPathExplicit(URI baseURI, HashType type) {
        this.baseURI = baseURI;
        this.type = type;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);

        URI remoteURI = baseURI;
        if (StringUtils.endsWith(baseURI.toString(), "data.zip")
                && !StringUtils.startsWith(baseURI.toString(), "zip:")) {
            remoteURI = HashKeyUtil.insertSlashIfNeeded(URI.create("zip:" + baseURI + "!/data/"), suffix);
        }

        return remoteURI;
    }

}
