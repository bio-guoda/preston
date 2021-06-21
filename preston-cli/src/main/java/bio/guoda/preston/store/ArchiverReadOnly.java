package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.getVersionSource;

public class ArchiverReadOnly extends VersionProcessor {

    private final StatementStoreReadOnly provenanceLogIndex;

    public ArchiverReadOnly(StatementStoreReadOnly provenanceLogIndex, StatementsListener... listeners) {
        super(listeners);
        this.provenanceLogIndex = provenanceLogIndex;
    }

    @Override
    void handleBlankVersion(Quad statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        VersionUtil.findMostRecentVersion(versionSource, getProvenanceLogIndex(), this::emit);
    }

    private StatementStoreReadOnly getProvenanceLogIndex() {
        return provenanceLogIndex;
    }

}
