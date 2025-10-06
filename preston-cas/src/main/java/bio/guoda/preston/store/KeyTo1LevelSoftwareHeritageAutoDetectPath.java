package bio.guoda.preston.store;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;

public class KeyTo1LevelSoftwareHeritageAutoDetectPath extends KeyTo1LevelSoftwareHeritagePath {

    public static final String SOFTWARE_HERITAGE_API_ENDPOINT = "https://archive.softwareheritage.org/api/1/content/";

    public KeyTo1LevelSoftwareHeritageAutoDetectPath(URI baseURI) {
        super(detectPath(baseURI));
    }

    private static URI detectPath(URI remote) {
        return StringUtils.contains(remote.getHost(), "softwareheritage.org")
                && (StringUtils.equals(remote.getPath(), "/") || StringUtils.isBlank(remote.getPath()))
                ? URI.create(SOFTWARE_HERITAGE_API_ENDPOINT)
                : remote;
    }

}
