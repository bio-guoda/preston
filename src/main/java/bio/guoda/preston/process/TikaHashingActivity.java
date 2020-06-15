package bio.guoda.preston.process;

import bio.guoda.preston.HashGeneratorTikaTLSH;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.jena.ontology.OntDocumentManager;

import java.io.IOException;
import java.util.UUID;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class TikaHashingActivity extends ProcessorReadOnly {

    public TikaHashingActivity(BlobStoreReadOnly blobStoreReadOnly, StatementListener listener) {
        super(blobStoreReadOnly, listener);
    }

    @Override
    public void on(Quad statement) {
        handleTerm(statement, statement.getObject());
        handleTerm(statement, statement.getSubject());
    }

    public void handleTerm(Quad statement, RDFTerm obj) {
        if (obj instanceof IRI) {
            handleIRI(statement, (IRI) obj);
        }
    }

    public void handleIRI(Quad statement, IRI obj) {
        IRI sourceIRI = obj;
        if (sourceIRI.getIRIString().startsWith("hash://sha256/")) {
            try {
                IRI tikaIRI = new HashGeneratorTikaTLSH().hash(get(sourceIRI));

                IRI activityUUID = toIRI(UUID.randomUUID());
                Stream<Quad> quadStream = Stream.of(
                        toStatement(tikaIRI, RefNodeConstants.WAS_DERIVED_FROM, sourceIRI),
                        toStatement(activityUUID, RefNodeConstants.USED, sourceIRI),
                        toStatement(tikaIRI, RefNodeConstants.WAS_GENERATED_BY, activityUUID));

                ActivityUtil.emitAsNewNamedActivity(quadStream, this, statement.getGraphName(), activityUUID);

            } catch (IOException e) {
                // problem calculating tika-hash
                // throw new RuntimeException("boom!", e);
            }
        }
    }
}
