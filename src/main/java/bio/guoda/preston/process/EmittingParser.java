package bio.guoda.preston.process;

import java.io.IOException;
import java.io.InputStream;

public interface EmittingParser {
    void parse(InputStream is, StatementEmitter emitter) throws IOException;
}
