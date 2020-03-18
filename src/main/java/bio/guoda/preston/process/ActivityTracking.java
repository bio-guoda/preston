package bio.guoda.preston.process;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.*;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class ActivityTracking {

    public static BlankNodeOrIRI beginInformedActivity(StatementEmitter emitter, Optional<BlankNodeOrIRI> sourceActivity) {
        List<Quad> statements = new LinkedList<Quad>();
        BlankNodeOrIRI newActivity = sourceActivity.orElse(toIRI(UUID.randomUUID()));
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
}
