package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.net.URI;

public class KeyTo3LevelZipPathExplicit extends KeyToPathAcceptsAnyValid {

    private final URI remote;
    private final HashType type;


    public KeyTo3LevelZipPathExplicit(URI remote, HashType type) {
        this.remote = remote;
        this.type = type;
    }

    @Override
    public URI toPath(IRI key) {
        HashKeyUtil.validateHashKey(key);

        String suffix = HashKeyUtil.pathSuffixForKey(key, type);

        URI remoteURI = remote;
        if (StringUtils.endsWith(remote.toString(), "data.zip")
                && !StringUtils.startsWith(remote.toString(), "zip:")) {
            remoteURI = HashKeyUtil.insertSlashIfNeeded(URI.create("zip:" + remote + "!/data/"), suffix);
        }

        return remoteURI;
    }

}
