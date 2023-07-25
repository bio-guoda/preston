package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.getVersionSource;

public class ArchiverReadOnly extends VersionProcessor {

    private final ProvenanceTracer provenanceTracer;

    public ArchiverReadOnly(ProvenanceTracer provenanceTracer, StatementsListener... listeners) {
        super(listeners);
        this.provenanceTracer = provenanceTracer;
    }

    @Override
    void handleBlankVersion(Quad statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        provenanceTracer.trace(versionSource, this::emit);
    }

}
