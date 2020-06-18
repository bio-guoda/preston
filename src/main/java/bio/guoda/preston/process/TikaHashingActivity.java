package bio.guoda.preston.process;

import bio.guoda.preston.HashGeneratorTikaTLSH;
import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class TikaHashingActivity extends ProcessorReadOnly {

    public TikaHashingActivity(BlobStoreReadOnly blobStoreReadOnly, StatementsListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        on(Collections.singletonList(statement));
    }

    private void handleHashes(Quad statement) {
        handleTerm(statement, statement.getSubject());
        handleTerm(statement, statement.getObject());
    }

    @Override
    public void on(List<Quad> statement) {
        if (statement.stream().noneMatch(q -> {
            RDFTerm subject = q.getSubject();
            return (subject instanceof IRI
                    && ((IRI) subject).getIRIString().startsWith("hash://tika-tlsh/"));
        })) {
            for (Quad quad : statement) {
                handleHashes(quad);
            }
        }
    }

    private void handleTerm(Quad statement, RDFTerm obj) {
        if (obj instanceof IRI) {
            handleIRI(statement, (IRI) obj);
        }
    }

    private void handleIRI(Quad statement, IRI sourceIRI) {
        if (sourceIRI.getIRIString().startsWith("hash://sha256/")) {
            try {
                IRI tikaIRI = new HashGeneratorTikaTLSH().hash(get(sourceIRI));

                IRI activityUUID = toIRI(UUID.randomUUID());
                Stream<Quad> quadStream = Stream.of(
                        toStatement(tikaIRI, RefNodeConstants.WAS_DERIVED_FROM, sourceIRI),
                        toStatement(activityUUID, RefNodeConstants.USED, sourceIRI),
                        toStatement(tikaIRI, RefNodeConstants.WAS_GENERATED_BY, activityUUID));

                ActivityUtil.emitAsNewActivity(quadStream, this, statement.getGraphName(), activityUUID);

            } catch (IOException e) {
                // problem calculating tika-hash
                // throw new RuntimeException("boom!", e);
            }
        }
    }
}
