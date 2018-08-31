package org.globalbioticinteractions.preston.process;

import java.io.IOException;
import java.io.InputStream;

public interface EmittingParser {
    void parse(InputStream is, RefStatementEmitter emitter) throws IOException;
}
