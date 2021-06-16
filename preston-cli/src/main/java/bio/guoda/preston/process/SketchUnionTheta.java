package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CONFIDENCE_INTERVAL_95;
import static bio.guoda.preston.RefNodeConstants.DESCRIPTION;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.THETA_SKETCH_PREFIX;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

/**
 * Reports on the approximate union of dataset elements via encountered Theta Sketches.
 *
 * see https://datasketches.apache.org/docs/Theta
 */

public class SketchUnionTheta extends ProcessorReadOnly implements Closeable {

    private Map<String, String> encounteredThetaSketches = new TreeMap<>();
    private IRI qualifiedGeneration;
    private Union unionSketch;
    private IRI activity;

    public SketchUnionTheta(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (WAS_DERIVED_FROM.equals(statement.getPredicate())
                && isOfHashType(statement.getSubject(), THETA_SKETCH_PREFIX)
                && isOfHashType(statement.getObject(), HashType.sha256.getPrefix())) {

            if (qualifiedGeneration == null || unionSketch == null) {
                qualifiedGeneration = toIRI(UUID.randomUUID());
                unionSketch = SetOperation.builder().buildUnion();
                activity = toIRI(UUID.randomUUID());
                Optional<BlankNodeOrIRI> parentActivity = statement.getGraphName();
                Stream<Quad> activityStart = ActivityUtil.beginInformedActivity(activity, parentActivity);
                Quad description = toStatement(activity, DESCRIPTION, toLiteral("estimate of distinct elements for union of encountered pre-computed theta sketches"));
                ActivityUtil.emitWithActivityName(Stream.concat(activityStart, Stream.of(description)),
                        this,
                        activity);
            }

            IRI sketchHash = (IRI) statement.getSubject();
            IRI shaHash = (IRI) statement.getObject();

            String iriString = shaHash.getIRIString();
            encounteredThetaSketches.put(iriString, sketchHash.getIRIString());

            try {
                Sketch sketchCurrent = SketchIntersectTheta.readSketch(sketchHash, this);
                if (sketchCurrent != null) {
                    unionSketch.union(sketchCurrent);
                    ActivityUtil.emitWithActivityName(
                            Stream.of(toStatement(qualifiedGeneration, USED, sketchHash)),
                            this,
                            activity);
                }
            } catch (IOException e) {
                throw new RuntimeException("cannot retrieve referenced sketch [" + sketchHash + "]", e);
            }
        }
    }

    private boolean isOfHashType(RDFTerm object, String prefix) {
        return object instanceof IRI
                && ((IRI) object).getIRIString().startsWith(prefix);
    }

    @Override
    public void close() throws IOException {
        if (unionSketch != null && qualifiedGeneration != null) {
            IRI similarityId = toIRI(UUID.randomUUID());
            IRI generationId = qualifiedGeneration;
            CompactSketch result = unionSketch.getResult();
            Stream<Quad> unionStatements = Stream.of(
                    toStatement(similarityId, HAS_VALUE, SketchIntersectTheta.toDoubleLiteral(result.getEstimate())),
                    toStatement(similarityId, CONFIDENCE_INTERVAL_95, SketchIntersectTheta.toDoubleLiteral(result.getUpperBound(2))),
                    toStatement(similarityId, QUALIFIED_GENERATION, generationId));

            ActivityUtil.emitWithActivityName(unionStatements, this, activity);
            unionSketch.reset();
            unionSketch = null;
            qualifiedGeneration = null;
        }
    }
}
