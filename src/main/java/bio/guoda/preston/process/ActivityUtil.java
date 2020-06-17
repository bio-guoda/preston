package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class ActivityUtil {

    private static void beginInformedActivity(StatementsEmitter emitter, BlankNodeOrIRI newActivity, Optional<BlankNodeOrIRI> sourceActivity) {
        emitter.emit(toStatement(newActivity, newActivity, IS_A, ACTIVITY));

        if (sourceActivity.isPresent()) {
            emitter.emit(toStatement(newActivity, newActivity, WAS_INFORMED_BY, sourceActivity.get()));
        }
    }

    private static void endInformedActivity(StatementEmitter emitter, BlankNodeOrIRI activity) {
    }

    private static void emitWithActivityName(Stream<Quad> quadStream, StatementsEmitter emitter, BlankNodeOrIRI activity) {
        quadStream.map(quad -> toStatement(activity, quad.getSubject(), quad.getPredicate(), quad.getObject()))
                .forEach(emitter::emit);
    }

    public static BlankNodeOrIRI emitAsNewActivity(Stream<Quad> quadStream, StatementsEmitter emitter, Optional<BlankNodeOrIRI> parentActivity) {
        BlankNodeOrIRI newActivity = toIRI(UUID.randomUUID());
        emitAsNewNamedActivity(quadStream, emitter, parentActivity, newActivity);
        return newActivity;
    }

    public static void emitAsNewNamedActivity(Stream<Quad> quadStream, StatementsEmitter emitter, Optional<BlankNodeOrIRI> parentActivity, BlankNodeOrIRI activityName) {
        beginInformedActivity(emitter, activityName, parentActivity);
        emitWithActivityName(quadStream, emitter, activityName);
        endInformedActivity(emitter, activityName);
    }
}
