package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.index.SimilarityIndexTikaTLSH;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;

public class SimilarContentFinder extends ProcessorReadOnly {

    private SimilarityIndexTikaTLSH similarityIndex;

    public SimilarContentFinder(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
        similarityIndex = new SimilarityIndexTikaTLSH();
    }

    @Override
    public void on(Quad statement) {
        if (statement.getPredicate().equals(WAS_DERIVED_FROM) &&
                statement.getSubject().ntriplesString().startsWith(HashType.tika_tlsh.getPrefix()) &&
                statement.getObject().ntriplesString().startsWith(HashType.sha256.getPrefix())) {

            IRI tlshHash = (IRI) statement.getSubject();
            IRI shaHash = (IRI) statement.getObject();

            similarityIndex.indexHashPair(shaHash, tlshHash);
            similarityIndex.getSimilarContents(tlshHash, 10)
                    .forEach(hit -> emitSimilarityRelationship(hit.getSHA256(), hit.getScore()));
        }
    }

    private void emitSimilarityRelationship(IRI sha256, float similarityScore) {
        // Emit stuff
    }
}
