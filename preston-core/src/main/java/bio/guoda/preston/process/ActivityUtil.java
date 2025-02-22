package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.ActivityContext;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.ACTIVITY;
import static bio.guoda.preston.RefNodeConstants.AGENT;
import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.GENERATED_AT_TIME;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.IS_A;
import static bio.guoda.preston.RefNodeConstants.SOFTWARE_AGENT;
import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.RefNodeConstants.WAS_GENERATED_BY;
import static bio.guoda.preston.RefNodeConstants.WAS_INFORMED_BY;
import static bio.guoda.preston.RefNodeFactory.toEnglishLiteral;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class ActivityUtil {

    public static Stream<Quad> beginInformedActivity(BlankNodeOrIRI newActivity, Optional<BlankNodeOrIRI> sourceActivity) {
        Stream<Quad> quadStream = Stream.of(isAnActivity(newActivity));

        return sourceActivity
                .map(activity -> Stream.concat(quadStream, Stream.of(toStatement(newActivity, newActivity, WAS_INFORMED_BY, activity))))
                .orElse(quadStream);
    }

    private static Quad isAnActivity(BlankNodeOrIRI newActivity) {
        return toStatement(newActivity, newActivity, IS_A, ACTIVITY);
    }

    public static void emitWithActivityName(Stream<Quad> quadStream, StatementsEmitter emitter, BlankNodeOrIRI activity) {
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

    public static void emitDownloadActivity(IRI versionSource, BlankNodeOrIRI newVersion, StatementsEmitter emitter, Optional<BlankNodeOrIRI> sourceActivity) {
        Literal dateTime = RefNodeFactory.nowDateTimeLiteral();

        IRI downloadActivity = toIRI(UUID.randomUUID());

        emitDownloadActivity(
                versionSource,
                newVersion,
                emitter,
                sourceActivity,
                dateTime,
                downloadActivity);
    }

    private static void emitDownloadActivity(IRI versionSource,
                                             BlankNodeOrIRI newVersion,
                                             StatementsEmitter emitter,
                                             Optional<BlankNodeOrIRI> sourceActivity,
                                             Literal dateTime,
                                             IRI downloadActivity) {
        emitter.emit(toStatement(
                downloadActivity,
                newVersion,
                WAS_GENERATED_BY,
                downloadActivity));
        emitter.emit(toStatement(
                downloadActivity,
                newVersion,
                RefNodeConstants.QUALIFIED_GENERATION,
                downloadActivity));
        emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                GENERATED_AT_TIME,
                dateTime));
        emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                IS_A,
                RefNodeConstants.GENERATION));
        sourceActivity.ifPresent(blankNodeOrIRI -> emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                RefNodeConstants.WAS_INFORMED_BY,
                blankNodeOrIRI)));
        emitter.emit(toStatement(
                downloadActivity,
                downloadActivity,
                RefNodeConstants.USED,
                versionSource));
        emitter.emit(toStatement(downloadActivity, versionSource, HAS_VERSION, newVersion));
    }

    public static List<Quad> generateSoftwareAgentProcessDescription(
            ActivityContext ctx,
            IRI softwareAgent,
            IRI softwareAgentDOI,
            String softwareAgentCitation,
            String softwareAgentDescription) {

        IRI activityId = ctx.getActivity();

        return Arrays.asList(
                toStatement(activityId, softwareAgent, IS_A, SOFTWARE_AGENT),
                toStatement(activityId, softwareAgent, IS_A, AGENT),
                toStatement(activityId, softwareAgent, DESCRIPTION, toEnglishLiteral(softwareAgentDescription)),


                isAnActivity(activityId),
                toStatement(activityId, activityId, DESCRIPTION, toEnglishLiteral(ctx.getDescription())),
                toStatement(activityId, activityId, RefNodeConstants.STARTED_AT_TIME, RefNodeFactory.nowDateTimeLiteral()),
                toStatement(activityId, activityId, RefNodeConstants.WAS_STARTED_BY, softwareAgent),


                toStatement(activityId, softwareAgentDOI, USED_BY, activityId),
                toStatement(activityId, softwareAgentDOI, IS_A, toIRI("http://purl.org/dc/dcmitype/Software")),
                toStatement(activityId, softwareAgentDOI,
                        toIRI("http://purl.org/dc/terms/bibliographicCitation"),
                        toEnglishLiteral(softwareAgentCitation)),

                toStatement(activityId, BIODIVERSITY_DATASET_GRAPH, IS_A, RefNodeConstants.ENTITY),
                toStatement(activityId, BIODIVERSITY_DATASET_GRAPH, DESCRIPTION, toEnglishLiteral("A biodiversity dataset graph archive."))
        );
    }

    public static ActivityContext createNewActivityContext(String activityDescription) {
        return new ActivityContext() {
            private final IRI activity = toIRI(UUID.randomUUID());

            @Override
            public IRI getActivity() {
                return activity;
            }

            @Override
            public String getDescription() {
                return activityDescription;
            }

            ;

        };
    }
}
