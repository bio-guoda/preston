package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.rdf.api.Quad;

import java.util.function.Predicate;

public class SelectiveListener extends StatementsListenerAdapter {

    private final Predicate<Quad> selector;
    private final StatementsListener listener;

    public SelectiveListener(Predicate<Quad> selector, StatementsListener listener) {
        this.selector = selector;
        this.listener = listener;
    }

    @Override
    public void on(Quad statement) {
        if (selector.test(statement)) {
            listener.on(statement);
        }
    }

}
