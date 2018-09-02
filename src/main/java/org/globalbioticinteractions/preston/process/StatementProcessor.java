package org.globalbioticinteractions.preston.process;


import org.apache.commons.rdf.api.Triple;

public abstract class StatementProcessor implements RefStatementListener, RefStatementEmitter {

    private final RefStatementListener[] listeners;

    public StatementProcessor(RefStatementListener... listeners) {
        this.listeners = listeners;
    }

    @Override
    public void emit(Triple statement) {
        for (RefStatementListener listener : listeners) {
            listener.on(statement);
        }
    }

}
