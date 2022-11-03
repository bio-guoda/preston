package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.StatementProcessor;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.function.Function;

public class RDFReader extends StatementProcessor {

    private final IRI qualifiedGeneration;
    private final BlobStore blobStore;
    private final Function<IRI, StatementsEmitter> processorSupplier;
    private final ProcessorState context;

    public RDFReader(BlobStore blobStore, StatementsListener listener, IRI qualifiedGeneration, Function<IRI, StatementsEmitter> processorSupplier, ProcessorState context) {
        super(listener);
        this.qualifiedGeneration = qualifiedGeneration;
        this.blobStore = blobStore;
        this.processorSupplier = processorSupplier;
        this.context = context;
    }

    @Override
    public void on(Quad statement) {
        getLatestVersion(statement).ifPresent(this::getAndProcessRDF);
    }

    private void getAndProcessRDF(IRI version) {
        emit(RefNodeFactory.toStatement(
                qualifiedGeneration,
                qualifiedGeneration,
                RefNodeConstants.USED,
                version
        ));

        try {
            processRDF(processorSupplier.apply(version), blobStore.get(version));
        } catch (IOException e) {
            throw new RuntimeException("Failed to dereference " + version, e);
        }
    }

    private void processRDF(StatementsEmitter quadProcessor, InputStream inputStream) {
        new EmittingStreamRDF(quadProcessor, context).parseAndEmit(inputStream);
    }

    private Optional<IRI> getLatestVersion(Quad statement) {
        if (statement.getPredicate().equals(RefNodeConstants.HAS_VERSION)) {
            return Optional.of((IRI) statement.getObject());
        } else if (statement.getPredicate().equals(RefNodeConstants.HAS_PREVIOUS_VERSION)) {
            return Optional.of((IRI) statement.getSubject());
        } else {
            return Optional.empty();
        }
    }

}
