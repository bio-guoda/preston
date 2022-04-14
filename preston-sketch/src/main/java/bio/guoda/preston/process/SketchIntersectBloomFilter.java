package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.simple.Types;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;

import static bio.guoda.preston.RefNodeConstants.*;
import static bio.guoda.preston.RefNodeConstants.BLOOM_HASH_PREFIX;
import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.OVERLAPS;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;

/**
 * Reports on the approximate overlap of dataset elements via encountered bloom filters.
 */

public class SketchIntersectBloomFilter extends ProcessorReadOnly implements Closeable {

    private Map<String, String> encounteredBloomFilters = new TreeMap<>();

    public static boolean enableCompression = true;


    public SketchIntersectBloomFilter(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        if (WAS_DERIVED_FROM.equals(statement.getPredicate())
                && isOfHashType(statement.getSubject(), BLOOM_HASH_PREFIX)
                && isOfHashType(statement.getObject(), HashType.sha256.getPrefix())) {

            IRI bloomGzHash = (IRI) statement.getSubject();
            IRI shaHash = (IRI) statement.getObject();

            String iriString = shaHash.getIRIString();
            encounteredBloomFilters.put(iriString, bloomGzHash.getIRIString());

            try {
                BloomFilter<CharSequence> bloomFilterCurrent = getBloomFilter(bloomGzHash);
                if (bloomFilterCurrent != null) {
                    long sizeReference = bloomFilterCurrent.approximateElementCount();

                    Map<String, Pair<Long, Double>> checkedTargets = new TreeMap<>();
                    for (String contentKey : encounteredBloomFilters.keySet()) {
                        if (!StringUtils.equals(iriString, contentKey)) {
                            IRI bloomGzHashTarget = toIRI(encounteredBloomFilters.get(contentKey));

                            if (!checkedTargets.containsKey(bloomGzHashTarget.getIRIString())) {
                                checkedTargets.put(
                                        bloomGzHashTarget.getIRIString(),
                                        calculateApproximateIntersection(bloomFilterCurrent, sizeReference, bloomGzHashTarget)
                                );
                            }

                            emitOverlap(
                                    Pair.of(bloomGzHash, shaHash),
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

    private Pair<Long,Double> calculateApproximateIntersection(BloomFilter<CharSequence> bloomFilterCurrent, long sizeReference, IRI bloomGzHashTarget) throws IOException {
        BloomFilter<CharSequence> bloomFilterExisting = getBloomFilter(bloomGzHashTarget);
        long sizeTarget = bloomFilterExisting.approximateElementCount();
        bloomFilterExisting.putAll(bloomFilterCurrent);

        long sizeCombined = bloomFilterExisting.approximateElementCount();

        return Pair.of((sizeReference + sizeTarget) - sizeCombined, bloomFilterExisting.expectedFpp());
    }

    private boolean isOfHashType(RDFTerm object, String prefix) {
        return object instanceof IRI
                && ((IRI) object).getIRIString().startsWith(prefix);
    }

    private BloomFilter<CharSequence> getBloomFilter(IRI contentHash) throws IOException {
        BloomFilter<CharSequence> bloomFilter = null;
        String bloomFilterContentId = StringUtils.removeStart(contentHash.getIRIString(), BLOOM_HASH_PREFIX);
        if (StringUtils.isNotBlank(bloomFilterContentId)) {
            try (InputStream inputStream = get(toIRI(bloomFilterContentId))) {
                if (inputStream == null) {
                    throw new IOException("failed to retrieve bloom filter [" + contentHash.getIRIString() + "]");
                }
                bloomFilter = BloomFilter.readFrom(
                        enableCompression ? new GZIPInputStream(inputStream) : inputStream,
                        Funnels.stringFunnel(StandardCharsets.UTF_8));
            }
        }
        return bloomFilter;
    }

    private void emitOverlap(Pair<IRI, IRI> leftBloomAndContent,
                             Pair<Long,Double> approximateIntersection,
                             Pair<IRI, IRI> rightBloomAndContent,
                             Optional<BlankNodeOrIRI> parentActivity) {
        IRI similarityId = toIRI(UUID.randomUUID());
        IRI generationId = toIRI(UUID.randomUUID());
        Stream<Quad> intersectionStatements = Stream.of(
                toStatement(similarityId, HAS_VALUE, toLiteral(Long.toString(approximateIntersection.getLeft()), Types.XSD_LONG)),
                toStatement(similarityId, STATISTICAL_ERROR, errorApproximationValue(approximateIntersection.getRight())),
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

    public static Literal errorApproximationValue(Double right) {
        return toLiteral(String.format("%.2f", right), Types.XSD_DOUBLE);
    }

    @Override
    public void close() throws IOException {
        encounteredBloomFilters.clear();
    }
}
