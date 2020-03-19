package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class ActivityUtil {

    public static BlankNodeOrIRI beginInformedActivity(StatementEmitter emitter, Optional<BlankNodeOrIRI> parentActivity) {
        List<Quad> statements = new LinkedList<Quad>();
        BlankNodeOrIRI newActivity = parentActivity.orElse(null);
//        IRI newActivity = toIRI(UUID.randomUUID());

//        statements.add(toStatement(newActivity, newActivity, IS_A, ACTIVITY));

//        if (sourceActivity.isPresent()) {
//            statements.add(
//                    toStatement(newActivity, newActivity, WAS_INFORMED_BY, sourceActivity.get())
//            );
//        }

//        statements.add(toStatement(newActivity, newActivity, STARTED_AT, time));
        statements.forEach(emitter::emit);

        return newActivity;
    }

    public static void endInformedActivity(StatementEmitter emitter, BlankNodeOrIRI activity) {
//        emitter.emit(toStatement(activity, activity, toIRI("http://www.w3.org/ns/prov#endedAtTime"), RefNodeFactory.nowDateTimeLiteral()));
    }

    public static void emitWithActivityName(Stream<Quad> quadStream, StatementEmitter emitter, BlankNodeOrIRI activity) {
        quadStream.map(quad -> toStatement(activity, quad.getSubject(), quad.getPredicate(), quad.getObject()))
                .forEach(emitter::emit);
    }

    public static void emitAsNewActivity(Stream<Quad> quadStream, StatementEmitter emitter, Optional<BlankNodeOrIRI> parentActivity) {
        BlankNodeOrIRI activity = beginInformedActivity(emitter, parentActivity);
        emitWithActivityName(quadStream, emitter, activity);
        endInformedActivity(emitter, activity);
    }
}
