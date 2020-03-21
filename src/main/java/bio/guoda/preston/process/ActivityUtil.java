package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.ENDED_AT_TIME;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.STARTED_AT_TIME;
import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class ActivityUtil {

    private static final boolean emitStartedAt = false;
    private static final boolean emitEndedAt = false;

    private static BlankNodeOrIRI beginInformedActivity(StatementEmitter emitter, Optional<BlankNodeOrIRI> sourceActivity) {
        BlankNodeOrIRI newActivity = toIRI(UUID.randomUUID());
        emitter.emit(toStatement(newActivity, newActivity, IS_A, ACTIVITY));

        if (sourceActivity.isPresent()) {
            emitter.emit(toStatement(newActivity, newActivity, WAS_INFORMED_BY, sourceActivity.get()));
        }

        if (emitStartedAt) {
            emitter.emit(toStatement(newActivity, newActivity, STARTED_AT_TIME, RefNodeFactory.nowDateTimeLiteral()));
        }

        return newActivity;
    }

    private static void endInformedActivity(StatementEmitter emitter, BlankNodeOrIRI activity) {
        if (emitEndedAt) {
            emitter.emit(toStatement(activity, activity, ENDED_AT_TIME, RefNodeFactory.nowDateTimeLiteral()));
        }
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
