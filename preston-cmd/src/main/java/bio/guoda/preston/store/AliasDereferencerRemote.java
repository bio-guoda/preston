package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.cmd.AliasUtil;
import bio.guoda.preston.cmd.Persisting;
import bio.guoda.preston.cmd.ProcessorStateImpl;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class AliasDereferencerRemote implements BlobStoreReadOnly {

    private final Dereferencer<InputStream> proxy;
    private final Persisting persisting;
    private final ProvenanceTracer provenanceTracer;

    public AliasDereferencerRemote(Dereferencer<InputStream> proxy, Persisting persisting, ProvenanceTracer tracer) {
        this.persisting = persisting;
        this.proxy = proxy;
        this.provenanceTracer = tracer;
    }

    @Override
    public InputStream get(IRI iri) throws DereferenceException {
        final AtomicReference<IRI> firstAliasHash = new AtomicReference<>(null);

        if (HashKeyUtil.isLikelyCompositeHashURI(iri)) {
            firstAliasHash.set(iri);
        } else {
            attemptToFindAlias(
                    firstAliasHash,
                    AliasUtil.aliasSelectorFor(iri)
            );
            attemptToFindInnerAlias(firstAliasHash, iri);
        }

        return firstAliasHash.get() == null
                ? null :
                dereferenceAliasedHash(iri, firstAliasHash);
    }

    private void attemptToFindInnerAlias(AtomicReference<IRI> firstAliasHash, IRI iri) {
        if (firstAliasHash.get() == null) {
            if (HashKeyUtil.isLikelyCompositeURI(iri)) {
                IRI innerAlias = HashKeyUtil.extractInnerURI(iri);
                if (!innerAlias.equals(iri)) {
                    attemptToFindAlias(
                            firstAliasHash,
                            AliasUtil.aliasSelectorFor(innerAlias)
                    );
                    if (firstAliasHash.get() != null) {
                        String compositeHashURIString = StringUtils.replace(iri.getIRIString(), innerAlias.getIRIString(), firstAliasHash.get().getIRIString());
                        firstAliasHash.set(RefNodeFactory.toIRI(compositeHashURIString));
                    }
                }
            }

        }
    }

    private void attemptToFindAlias(AtomicReference<IRI> firstAliasHash,
                                    Predicate<Quad> selector
    ) {
        ProcessorState aliasFindingState = new ProcessorStateImpl();

        AliasUtil.findSelectedAlias(
                new StatementsListenerAdapter() {
                    @Override
                    public void on(Quad statement) {
                        if (aliasFindingState.shouldKeepProcessing()) {
                            if (statement.getObject() instanceof IRI) {
                                firstAliasHash.set((IRI) statement.getObject());
                                aliasFindingState.stopProcessing();
                            }
                        }
                    }
                },
                selector,
                persisting,
                provenanceTracer);
    }

    private InputStream dereferenceAliasedHash(IRI iri, AtomicReference<IRI> firstAliasHash) throws DereferenceException {
        try {
            return proxy.get(firstAliasHash.get());
        } catch (IOException | IllegalArgumentException e) {
            throw new DereferenceException(iri, e);
        }
    }

    private class AliasFindingState implements ProcessorState {

        private final ProcessorState state;
        private boolean foundAlias = false;

        AliasFindingState(ProcessorState state) {
            this.state = state;
        }

        void foundAlias() {
            foundAlias = true;
        }

        @Override
        public void stopProcessing() {
            state.stopProcessing();
        }

        @Override
        public boolean shouldKeepProcessing() {
            return !foundAlias && state.shouldKeepProcessing();
        }
    }
}
