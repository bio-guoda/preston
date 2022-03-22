package bio.guoda.preston.store;

import org.apache.commons.lang3.StringUtils;

import java.net.URI;

public class KeyTo1LevelSoftwareHeritageAutoDetectPath extends KeyTo1LevelSoftwareHeritagePath {

    public KeyTo1LevelSoftwareHeritageAutoDetectPath(URI baseURI) {
        super(detectPath(baseURI));
    }

    private static URI detectPath(URI baseURI) {
        return StringUtils.contains(baseURI.getHost(), "softwareheritage.org")
                && !StringUtils.endsWith(baseURI.getPath(), "sha256:")
                ? URI.create("https://archive.softwareheritage.org/api/1/content/sha256:")
                : baseURI;
    }

}
