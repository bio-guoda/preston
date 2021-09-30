package bio.guoda.preston.store;

import bio.guoda.preston.cmd.AliasUtil;
import bio.guoda.preston.cmd.Cmd;
import bio.guoda.preston.cmd.Persisting;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicReference;

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
            AliasUtil.findSelectedAlias(new StatementsListenerAdapter() {
                @Override
                public void on(Quad statement) {
                    if (statement.getObject() instanceof IRI) {
                        firstAliasHash.set((IRI) statement.getObject());
                        Cmd.stopProcessing();
                    }
                }
            }, q -> q.getSubject().equals(iri), persisting);
        }

        return firstAliasHash.get() == null
                ? null :
                dereferenceAliasedHash(iri, firstAliasHash);
    }

    private InputStream dereferenceAliasedHash(IRI iri, AtomicReference<IRI> firstAliasHash) throws DereferenceException {
        try {
            return proxy.get(firstAliasHash.get());
        } catch (IOException | IllegalArgumentException e) {
            throw new DereferenceException(iri, e);
        }
    }

}
