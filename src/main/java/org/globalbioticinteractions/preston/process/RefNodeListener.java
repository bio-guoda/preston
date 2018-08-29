package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

public interface RefNodeListener {
    void on(RefStatement relation);
}
