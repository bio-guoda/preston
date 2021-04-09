package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.simple.Types;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.CompactSketch;
import org.apache.datasketches.theta.Intersection;
import org.apache.datasketches.theta.SetOperation;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Sketches;
import org.apache.tika.io.IOUtils;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CONFIDENCE_INTERVAL_95;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.OVERLAPS;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.THETA_SKETCH_PREFIX;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

/**
 * Reports on the approximate overlap of dataset elements via encountered Theta Sketches.
 *
 * see https://datasketches.apache.org/docs/Theta
 */

public class SketchIntersectTheta extends ProcessorReadOnly implements Closeable {

    private Map<String, String> encounteredThetaSketches = new TreeMap<>();

    public SketchIntersectTheta(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (WAS_DERIVED_FROM.equals(statement.getPredicate())
                && isOfHashType(statement.getSubject(), THETA_SKETCH_PREFIX)
                && isOfHashType(statement.getObject(), HashType.sha256.getPrefix())) {

            IRI sketchHash = (IRI) statement.getSubject();
            IRI shaHash = (IRI) statement.getObject();

            String iriString = shaHash.getIRIString();
            encounteredThetaSketches.put(iriString, sketchHash.getIRIString());

            try {
                Sketch bloomFilterCurrent = readSketch(sketchHash);
                if (bloomFilterCurrent != null) {
                    Map<String, Pair<Double, Double>> checkedTargets = new TreeMap<>();
                    for (String contentKey : encounteredThetaSketches.keySet()) {
                        if (!StringUtils.equals(iriString, contentKey)) {
                            IRI bloomGzHashTarget = toIRI(encounteredThetaSketches.get(contentKey));

                            if (!checkedTargets.containsKey(bloomGzHashTarget.getIRIString())) {
                                checkedTargets.put(
                                        bloomGzHashTarget.getIRIString(),
                                        calculateApproximateIntersection(bloomFilterCurrent, bloomGzHashTarget)
                                );
                            }

                            emitOverlap(
                                    Pair.of(sketchHash, shaHash),
                                    checkedTargets.get(bloomGzHashTarget.getIRIString()),
                                    Pair.of(bloomGzHashTarget, RefNodeFactory.toIRI(contentKey)),
                                    statement.getGraphName());
                        }
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException("cannot retrieve referenced bloom filter(s)", e);
            }
        }
    }

    private Pair<Double,Double> calculateApproximateIntersection(Sketch bloomFilterCurrent, IRI bloomGzHashTarget) throws IOException {
        Sketch bloomFilterExisting = readSketch(bloomGzHashTarget);
        Intersection intersection = SetOperation.builder().buildIntersection();
        CompactSketch intersect = intersection.intersect(bloomFilterCurrent, bloomFilterExisting);

        double sizeIntersect = intersect.getEstimate();

        return Pair.of(sizeIntersect, intersect.getLowerBound(2));
    }

    private boolean isOfHashType(RDFTerm object, String prefix) {
        return object instanceof IRI
                && ((IRI) object).getIRIString().startsWith(prefix);
    }

    private Sketch readSketch(IRI contentHash) throws IOException {
        Sketch sketch = null;
        String sketchContentId = StringUtils.removeStart(contentHash.getIRIString(), THETA_SKETCH_PREFIX);
        if (StringUtils.isNotBlank(sketchContentId)) {
            try (InputStream inputStream = get(toIRI(sketchContentId))) {
                if (inputStream == null) {
                    throw new IOException("failed to retrieve theta sketch [" + contentHash.getIRIString() + "]");
                }
                sketch = Sketches.wrapSketch(Memory.wrap(IOUtils.toByteArray(inputStream)));
            }
        }
        return sketch;
    }

    private void emitOverlap(Pair<IRI, IRI> leftBloomAndContent,
                             Pair<Double,Double> approximateIntersection,
                             Pair<IRI, IRI> rightBloomAndContent,
                             Optional<BlankNodeOrIRI> parentActivity) {
        IRI similarityId = toIRI(UUID.randomUUID());
        IRI generationId = toIRI(UUID.randomUUID());
        Stream<Quad> intersectionStatements = Stream.of(
                toStatement(similarityId, HAS_VALUE, toDoubleLiteral(approximateIntersection.getLeft())),
                toStatement(similarityId, CONFIDENCE_INTERVAL_95, toDoubleLiteral(approximateIntersection.getRight())),
                toStatement(similarityId, QUALIFIED_GENERATION, generationId),
                toStatement(generationId, USED, leftBloomAndContent.getRight()),
                toStatement(generationId, USED, leftBloomAndContent.getLeft()),
                toStatement(generationId, USED, rightBloomAndContent.getRight()),
                toStatement(generationId, USED, rightBloomAndContent.getLeft()));


        Stream<Quad> statements = approximateIntersection.getLeft() > 0
                ? Stream.concat(Stream.of(toStatement(leftBloomAndContent.getValue(), OVERLAPS, rightBloomAndContent.getValue())), intersectionStatements)
                : intersectionStatements;

        ActivityUtil.emitAsNewActivity(
                statements,
                this,
                parentActivity
        );
    }

    public static Literal toDoubleLiteral(Double right) {
        return toLiteral(String.format("%.2f", right), Types.XSD_DOUBLE);
    }

    @Override
    public void close() throws IOException {
        encounteredThetaSketches.clear();
    }
}
