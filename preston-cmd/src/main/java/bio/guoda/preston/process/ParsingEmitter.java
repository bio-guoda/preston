package bio.guoda.preston.process;

import java.io.InputStream;

public interface ParsingEmitter {
    void parseAndEmit(InputStream inputStream);
}
