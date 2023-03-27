package bio.guoda.preston.process;

import java.io.InputStream;

public interface EmittingStream {
    void parseAndEmit(InputStream inputStream);
}
