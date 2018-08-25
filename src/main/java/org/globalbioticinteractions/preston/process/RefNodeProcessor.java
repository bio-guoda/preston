package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;

public abstract class RefNodeProcessor implements RefNodeListener, RefNodeEmitter {

    private final RefNodeListener[] listeners;

    public RefNodeProcessor(RefNodeListener... listeners) {
        this.listeners = listeners;
    }

    @Override
    public void emit(RefNode refNode) {
        for (RefNodeListener listener : listeners) {
            listener.on(refNode);
        }
    }

}
