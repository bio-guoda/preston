package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeRelation;

public abstract class RefNodeProcessor implements RefNodeListener, RefNodeEmitter {

    private final RefNodeListener[] listeners;

    public RefNodeProcessor(RefNodeListener... listeners) {
        this.listeners = listeners;
    }

    @Override
    public void emit(RefNodeRelation refNode) {
        for (RefNodeListener listener : listeners) {
            listener.on(refNode);
        }
    }

}
