package bio.guoda.preston.process;

import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.Quad;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class ActivityUtil {

    private static Stream<Quad> beginInformedActivity(BlankNodeOrIRI newActivity, Optional<BlankNodeOrIRI> sourceActivity) {
        Stream<Quad> quadStream = Stream.of(toStatement(newActivity, newActivity, IS_A, ACTIVITY));

        return sourceActivity
                .map(activity -> Stream.concat(quadStream, Stream.of(toStatement(newActivity, newActivity, WAS_INFORMED_BY, activity))))
                .orElse(quadStream);
    }

    private static void emitWithActivityName(Stream<Quad> quadStream, StatementsEmitter emitter, BlankNodeOrIRI activity) {
        List<Quad> statements = quadStream
                .map(quad -> toStatement(activity, quad))
                .collect(Collectors.toList());
        emitter.emit(statements);
    }

    public static BlankNodeOrIRI emitAsNewActivity(Stream<Quad> quadStream, StatementsEmitter emitter, Optional<BlankNodeOrIRI> parentActivity) {
        BlankNodeOrIRI newActivity = toIRI(UUID.randomUUID());
        emitAsNewActivity(quadStream, emitter, parentActivity, newActivity);
        return newActivity;
    }

    public static void emitAsNewActivity(Stream<Quad> activityStatements, StatementsEmitter emitter, Optional<BlankNodeOrIRI> parentActivity, BlankNodeOrIRI activityName) {
        Stream<Quad> activityStart = beginInformedActivity(activityName, parentActivity);
        emitWithActivityName(Stream.concat(activityStart, activityStatements), emitter, activityName);
    }
}
