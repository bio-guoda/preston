package bio.guoda.preston.store;

import org.apache.commons.rdf.api.IRI;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface KeyGeneratingStream {
    IRI generateKeyWhileStreaming(InputStream is, OutputStream os) throws IOException;
}
