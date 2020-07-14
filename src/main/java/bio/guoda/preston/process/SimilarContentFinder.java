package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.index.SimilarityIndexTikaTLSH;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.simple.Types;

import java.io.File;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;
import static bio.guoda.preston.RefNodeConstants.QUALIFIED_GENERATION;
import static bio.guoda.preston.RefNodeConstants.USED;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class SimilarContentFinder extends ProcessorReadOnly {

    private static final float MIN_SIMILARITY_SCORE = 0.5f;
    private final int MAX_HITS = 10;

    private final SimilarityIndexTikaTLSH similarityIndex;

    public SimilarContentFinder(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener, File indexDir) {
        super(blobStoreReadOnly, listener);
        similarityIndex = new SimilarityIndexTikaTLSH(indexDir);
    }

    @Override
    public void on(Quad statement) {
        if (statement.getPredicate().equals(WAS_DERIVED_FROM) &&
                statement.getSubject() instanceof IRI && ((IRI)statement.getSubject()).getIRIString().startsWith(HashType.tika_tlsh.getPrefix()) &&
                statement.getObject() instanceof IRI && ((IRI)statement.getObject()).getIRIString().startsWith(HashType.sha256.getPrefix())) {

            IRI tlshHash = (IRI)statement.getSubject();
            IRI shaHash = (IRI)statement.getObject();

            similarityIndex.indexHashPair(shaHash, tlshHash);
            similarityIndex.getSimilarContents(tlshHash, MAX_HITS + 1)
                    .filter(hit -> !hit.getSHA256().equals(shaHash))
                    .filter(hit -> hit.getScore() >= MIN_SIMILARITY_SCORE)
                    .forEach(hit -> emitSimilarityRelationship(shaHash, hit.getSHA256(), hit.getScore(), statement.getGraphName()));
        }
    }

    private void emitSimilarityRelationship(IRI firstSha256, IRI secondSha256, float similarityScore, Optional<BlankNodeOrIRI> parentActivity) {
        IRI similarityId = toIRI(UUID.randomUUID());
        IRI generationId = toIRI(UUID.randomUUID());
        ActivityUtil.emitAsNewActivity(Stream.of(
                    toStatement(similarityId, HAS_VALUE, toLiteral(Float.toString(similarityScore), Types.XSD_FLOAT)),
                    toStatement(similarityId, QUALIFIED_GENERATION, generationId),
                    toStatement(generationId, USED, firstSha256),
                    toStatement(generationId, USED, secondSha256)),
                this,
                parentActivity
        );
    }
}
