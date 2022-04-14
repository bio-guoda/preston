package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;

public class AliasDereferencer implements BlobStoreReadOnly {

    private final Persisting persisting;
    private final Dereferencer<InputStream> proxy;

    public AliasDereferencer(Dereferencer<InputStream> proxy, Persisting persisting) {
        this.persisting = persisting;
        this.proxy = proxy;
    }

    @Override
    public InputStream get(IRI iri) throws DereferenceException {
        final AtomicReference<IRI> firstAliasHash = new AtomicReference<>(null);

        if (HashKeyUtil.isLikelyCompositeHashURI(iri)) {
            firstAliasHash.set(iri);
        } else {
            attemptToFindAlias(firstAliasHash, q -> q.getSubject().equals(iri), persisting);
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
                    attemptToFindAlias(firstAliasHash, q -> q.getSubject().equals(innerAlias), persisting);
                    if (firstAliasHash.get() != null) {
                        String compositeHashURIString = StringUtils.replace(iri.getIRIString(), innerAlias.getIRIString(), firstAliasHash.get().getIRIString());
                        firstAliasHash.set(RefNodeFactory.toIRI(compositeHashURIString));
                    }
                }
            }

        }
    }

    private void attemptToFindAlias(AtomicReference<IRI> firstAliasHash, Predicate<Quad> selector, final ProcessorState processorState) {
        AliasUtil.findSelectedAlias(new StatementsListenerAdapter() {
                                        @Override
                                        public void on(Quad statement) {
                                            if (statement.getObject() instanceof IRI) {
                                                firstAliasHash.set((IRI) statement.getObject());
                                                processorState.stopProcessing();
                                            }
                                        }
                                    },
                selector,
                persisting);
    }

    private InputStream dereferenceAliasedHash(IRI iri, AtomicReference<IRI> firstAliasHash) throws DereferenceException {
        try {
            return proxy.get(firstAliasHash.get());
        } catch (IOException | IllegalArgumentException e) {
            throw new DereferenceException(iri, e);
        }
    }

}
