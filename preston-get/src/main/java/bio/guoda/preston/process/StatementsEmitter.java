package bio.guoda.preston.process;


import org.apache.commons.rdf.api.Quad;

import java.util.List;

public interface StatementsEmitter extends StatementEmitter {
    void emit(List<Quad> statement);
}
