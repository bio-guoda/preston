package org.globalbioticinteractions.preston.process;


import org.apache.commons.rdf.api.Triple;

public abstract class StatementProcessor implements StatementListener, StatementEmitter {

    private final StatementListener[] listeners;

    public StatementProcessor(StatementListener... listeners) {
        this.listeners = listeners;
    }

    @Override
    public void emit(Triple statement) {
        for (StatementListener listener : listeners) {
            listener.on(statement);
        }
    }

}
