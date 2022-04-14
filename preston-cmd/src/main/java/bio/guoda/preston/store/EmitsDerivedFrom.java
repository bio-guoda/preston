package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.Collection;
import java.util.List;
import java.util.Queue;

import static bio.guoda.preston.RefNodeFactory.toStatement;

public class EmitsDerivedFrom extends StatementsEmitterAdapter {

    private final IRI origin;
    private final StatementListener listener;
    private final Collection<IRI> statementQueue;

    public EmitsDerivedFrom(IRI origin, StatementListener listener, Collection<IRI> statementQueue) {
        this.origin = origin;
        this.listener = listener;
        this.statementQueue = statementQueue;
    }

    @Override
    public void emit(Quad statement) {
        if (RefNodeConstants.USED_BY.equals(statement.getPredicate())) {
            if (statement.getSubject() instanceof IRI) {
                IRI subject = (IRI) statement.getSubject();
                if (HashKeyUtil.isValidHashKey(subject)) {
                    Quad originStatement =
                            toStatement(origin,
                                    RefNodeConstants.WAS_DERIVED_FROM,
                                    subject);
                    statementQueue.add(subject);
                    listener.on(originStatement);
                }
            }
        }
    }
}
