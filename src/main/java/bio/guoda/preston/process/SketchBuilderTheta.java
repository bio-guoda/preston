package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;
import org.apache.datasketches.theta.UpdateSketch;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.CONFIDENCE_INTERVAL_95;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.THETA_SKETCH_PREFIX;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

/**
 * Creates ThetaSketch filter from encountered content values.
 */

public class SketchBuilderTheta extends SketchBuilder {


    private final AtomicReference<Triple<String, UpdateSketch, Quad>> activeFilterContext = new AtomicReference<>();

    private final BlobStore blobStore;

    public SketchBuilderTheta(BlobStore blobStore, StatementsListener listener) {
        super(listener);
        this.blobStore = blobStore;
    }

    @Override
    protected void updateSketch(Quad statement, IRI contentId) {
        getActiveFilter(statement, contentId)
                .update(RDFUtil.getValueFor(statement.getObject()));
    }

    private UpdateSketch getActiveFilter(Quad statement, IRI contentId) {
        Triple<String, UpdateSketch, Quad> activeFilterContext = this.activeFilterContext.get();

        UpdateSketch filter;
        if (activeFilterContext == null) {
            filter = setNewActiveFilter(contentId, statement);
        } else if (StringUtils.equals(activeFilterContext.getLeft(), contentId.getIRIString())) {
            filter = activeFilterContext.getMiddle();
        } else {
            dereferenceSketch(activeFilterContext);
            filter = setNewActiveFilter(contentId, statement);
        }
        return filter;
    }

    private void dereferenceSketch(Triple<String, UpdateSketch, Quad> filterPair) {
        dereferenceFilter(
                filterPair.getRight(),
                RefNodeFactory.toIRI(filterPair.getLeft()),
                filterPair.getMiddle());
    }

    private UpdateSketch setNewActiveFilter(IRI contentId, Quad statement) {
        UpdateSketch filter = UpdateSketch.builder().build();

        Triple<String, UpdateSketch, Quad> filterContext
                = Triple.of(contentId.getIRIString(), filter, statement);
        activeFilterContext.set(filterContext);
        return filter;
    }

    private void dereferenceFilter(Quad statement, IRI contentId, UpdateSketch filter) {
        try {
            IRI sketchContentId = blobStore.put(new ByteArrayInputStream(filter.compact().toByteArray()));
            emitBloomFilter(
                    filter,
                    RefNodeFactory.toIRI(THETA_SKETCH_PREFIX + sketchContentId.getIRIString()),
                    contentId,
                    statement.getGraphName());
        } catch (IOException e) {
            throw new RuntimeException("failed to serialize bloom filter [bloom:gz:" + contentId.getIRIString() + "]", e);
        }
    }

    private void emitBloomFilter(UpdateSketch filter,
                                 IRI sketchHash,
                                 IRI contentId,
                                 Optional<BlankNodeOrIRI> parentActivity) {

        ActivityUtil.emitAsNewActivity(
                Stream.of(toStatement(sketchHash, WAS_DERIVED_FROM, contentId),
                        toStatement(sketchHash, HAS_VALUE, toLiteral(String.format("%.2f", filter.getEstimate()), Types.XSD_DOUBLE)),
                        toStatement(sketchHash, CONFIDENCE_INTERVAL_95, toLiteral(String.format("%.2f", filter.getLowerBound(2)), Types.XSD_DOUBLE))),
                this,
                parentActivity
        );
    }

    @Override
    public void close() throws IOException {
        Triple<String, UpdateSketch, Quad> filterContext = activeFilterContext.getAndSet(null);
        if (filterContext != null) {
            dereferenceSketch(filterContext);
        }
    }
}
