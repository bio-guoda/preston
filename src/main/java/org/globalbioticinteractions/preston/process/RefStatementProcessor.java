package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

public abstract class RefStatementProcessor implements RefStatementListener, RefStatementEmitter {

    private final RefStatementListener[] listeners;

    public RefStatementProcessor(RefStatementListener... listeners) {
        this.listeners = listeners;
    }

    @Override
    public void emit(RefStatement statement) {
        for (RefStatementListener listener : listeners) {
            listener.on(statement);
        }
    }

}
