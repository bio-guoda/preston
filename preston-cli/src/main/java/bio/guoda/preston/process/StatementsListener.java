package bio.guoda.preston.process;

import org.apache.commons.rdf.api.Quad;

import java.util.List;

public interface StatementsListener extends StatementListener {
    void on(List<Quad> statement);
}
