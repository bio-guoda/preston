package bio.guoda.preston.store;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface KeyGeneratingStream {
    String generateKeyWhileStreaming(InputStream is, OutputStream os) throws IOException;
}
