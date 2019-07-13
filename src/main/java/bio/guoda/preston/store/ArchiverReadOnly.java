package bio.guoda.preston.store;

import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;

import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;

public class ArchiverReadOnly extends VersionProcessor {

    private final StatementStoreReadOnly statementStore;

    public ArchiverReadOnly(StatementStoreReadOnly statementStore, StatementListener... listeners) {
        super(listeners);
        this.statementStore = statementStore;
    }

    @Override
    void handleBlankVersion(Triple statement, BlankNode blankVersion) throws IOException {
        IRI versionSource = getVersionSource(statement);
        VersionUtil.findMostRecentVersion(versionSource, getStatementStore(), new VersionListener() {

            @Override
            public void onVersion(Triple statement) throws IOException {
                emit(statement);
            }
        });
    }

    private StatementStoreReadOnly getStatementStore() {
        return statementStore;
    }

}
