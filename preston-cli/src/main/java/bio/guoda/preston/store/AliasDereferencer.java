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

public class AliasDereferencer implements Dereferencer<InputStream> {

    private final Persisting persisting;
    private final Dereferencer<InputStream> proxy;

    public AliasDereferencer(Dereferencer<InputStream> proxy, Persisting persisting) {
        this.persisting = persisting;
        this.proxy = proxy;
    }

    @Override
    public InputStream dereference(IRI iri) throws DereferenceException {
        final AtomicReference<IRI> firstAliasHash = new AtomicReference<>(null);

        if (HashKeyUtil.isValidHashKey(iri)) {
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
        if (firstAliasHash.get() == null) {
            throw new DereferenceException("failed to dereference " + iri + " : alias not found");
        } else {
            return dereferenceAliasedHash(iri, firstAliasHash);
        }
    }

    private InputStream dereferenceAliasedHash(IRI iri, AtomicReference<IRI> firstAliasHash) throws DereferenceException {
        try {
            return proxy.dereference(firstAliasHash.get());
        } catch (IOException | IllegalArgumentException e) {
            throw new DereferenceException(iri, e);
        }
    }

}