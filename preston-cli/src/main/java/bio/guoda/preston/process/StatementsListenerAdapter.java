package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.util.List;

public abstract class StatementsListenerAdapter implements StatementsListener {
    @Override
    public void on(List<Quad> statements) {
        for (Quad statement : statements) {
            on(statement);
        }
    }

}
