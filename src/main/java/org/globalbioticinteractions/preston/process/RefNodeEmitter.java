package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

public interface RefNodeEmitter {
    void emit(RefStatement relation);
}
