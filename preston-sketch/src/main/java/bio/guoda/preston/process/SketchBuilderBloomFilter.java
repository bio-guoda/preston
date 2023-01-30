package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import java.util.zip.GZIPOutputStream;

import static bio.guoda.preston.RefNodeConstants.BLOOM_HASH_PREFIX;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.STATISTICAL_ERROR;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;

/**
 * Creates bloom filter from encountered content values.
 */

public class SketchBuilderBloomFilter extends SketchBuilder {

    private final AtomicReference<Triple<String, BloomFilter<CharSequence>, Quad>> activeFilterContext = new AtomicReference<>();

    private final BlobStore blobStore;

    public SketchBuilderBloomFilter(BlobStore blobStore, StatementsListener listener) {
        super(listener);
        this.blobStore = blobStore;
    }

    @Override
    protected void updateSketch(Quad statement, IRI contentId) {
        getActiveFilter(statement, contentId)
                .put(RDFUtil.getValueFor(statement.getObject()));
    }

    private BloomFilter<CharSequence> getActiveFilter(Quad statement, IRI contentId) {
        Triple<String, BloomFilter<CharSequence>, Quad> activeFilterContext = this.activeFilterContext.get();

        BloomFilter<CharSequence> filter;
        if (activeFilterContext == null) {
            filter = setNewActiveFilter(contentId, statement);
        } else if (StringUtils.equals(activeFilterContext.getLeft(), contentId.getIRIString())) {
            filter = activeFilterContext.getMiddle();
        } else {
            deferenceBloomFilter(activeFilterContext);
            filter = setNewActiveFilter(contentId, statement);
        }
        return filter;
    }

    private void deferenceBloomFilter(Triple<String, BloomFilter<CharSequence>, Quad> filterPair) {
        dereferenceFilter(
                filterPair.getRight(),
                RefNodeFactory.toIRI(filterPair.getLeft()),
                filterPair.getMiddle());
    }

    private BloomFilter<CharSequence> setNewActiveFilter(IRI contentId, Quad statement) {
        BloomFilter<CharSequence> filter
                = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 10 * 1000000);
        Triple<String, BloomFilter<CharSequence>, Quad> filterContext
                = Triple.of(contentId.getIRIString(), filter, statement);
        activeFilterContext.set(filterContext);
        return filter;
    }

    private void dereferenceFilter(Quad statement, IRI contentId, BloomFilter<CharSequence> filter) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (OutputStream os = SketchIntersectBloomFilter.enableCompression ? new GZIPOutputStream(out) : out) {
                filter.writeTo(os);
            }
            IRI bloomFilterContentId = blobStore.put(new ByteArrayInputStream(out.toByteArray()));
            emitBloomFilter(
                    filter,
                    RefNodeFactory.toIRI(BLOOM_HASH_PREFIX + bloomFilterContentId.getIRIString()),
                    contentId,
                    statement.getGraphName());
        } catch (IOException e) {
            throw new RuntimeException("failed to serialize bloom filter [gz:bloom:" + contentId.getIRIString() + "]", e);
        }
    }

    private void emitBloomFilter(BloomFilter<CharSequence> filter,
                                 IRI bloomHash,
                                 IRI contentId,
                                 Optional<BlankNodeOrIRI> parentActivity) {

        ActivityUtil.emitAsNewActivity(
                Stream.of(toStatement(bloomHash, WAS_DERIVED_FROM, contentId),
                        toStatement(bloomHash, HAS_VALUE, toLiteral(Long.toString(filter.approximateElementCount()), Types.XSD_LONG)),
                        toStatement(bloomHash, STATISTICAL_ERROR, toLiteral(String.format("%.2f", filter.expectedFpp()), Types.XSD_DOUBLE))),
                this,
                parentActivity
        );
    }

    @Override
    public void close() throws IOException {
        Triple<String, BloomFilter<CharSequence>, Quad> filterContext = activeFilterContext.getAndSet(null);
        if (filterContext != null) {
            deferenceBloomFilter(filterContext);
        }
    }
}
