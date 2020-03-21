package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.jena.shared.uuid.JenaUUID;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class ActivityUtil {

    private static BlankNodeOrIRI beginInformedActivity(StatementEmitter emitter, Optional<BlankNodeOrIRI> parentActivity) {
        List<Quad> statements = new LinkedList<>();
        BlankNodeOrIRI newActivity = parentActivity.orElse(null);

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

    private static void endInformedActivity(StatementEmitter emitter, BlankNodeOrIRI activity) {
//        emitter.emit(toStatement(activity, activity, toIRI("http://www.w3.org/ns/prov#endedAtTime"), RefNodeFactory.nowDateTimeLiteral()));
    }

    private static void emitWithActivityName(Stream<Quad> quadStream, StatementEmitter emitter, BlankNodeOrIRI activity) {
        quadStream.map(quad -> toStatement(activity, quad.getSubject(), quad.getPredicate(), quad.getObject()))
                .forEach(emitter::emit);
    }

    public static BlankNodeOrIRI emitAsNewActivity(Stream<Quad> quadStream, StatementEmitter emitter, Optional<BlankNodeOrIRI> parentActivity) {
        BlankNodeOrIRI activity = beginInformedActivity(emitter, parentActivity);
        emitWithActivityName(quadStream, emitter, activity);
        endInformedActivity(emitter, activity);
        return activity;
    }
}
