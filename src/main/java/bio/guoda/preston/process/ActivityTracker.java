package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

public class ActivityTracker extends StatementProcessor {

    public ActivityTracker(StatementListener... listener) {
        super(listener);
    }

    @Override
    public void on(Quad statement) {
        emit(statement);
    }

}
