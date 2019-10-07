package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;

public class ArchiverReadOnly extends VersionProcessor {

    private final StatementStoreReadOnly provenanceLogIndex;

    public ArchiverReadOnly(StatementStoreReadOnly provenanceLogIndex, StatementListener... listeners) {
        super(listeners);
        this.provenanceLogIndex = provenanceLogIndex;
    }

    @Override
    void handleBlankVersion(Triple statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        VersionUtil.findMostRecentVersion(versionSource, getProvenanceLogIndex(), this::emit);
    }

    private StatementStoreReadOnly getProvenanceLogIndex() {
        return provenanceLogIndex;
    }

}
