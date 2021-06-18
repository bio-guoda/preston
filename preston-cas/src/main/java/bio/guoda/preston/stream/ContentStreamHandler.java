package bio.guoda.preston.stream;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;

public interface ContentStreamHandler {
    boolean handle(IRI version, InputStream in) throws ContentStreamException;

    boolean shouldKeepReading();
}
