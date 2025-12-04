package bio.guoda.preston.store;

import java.net.URI;

public class ContentAlternateException extends RuntimeException {
    private final URI alternatePath;

    public ContentAlternateException(URI path) {
        super(path.toString());
        this.alternatePath = path;
    }

    public URI getAlternatePath() {
        return alternatePath;
    }
}
