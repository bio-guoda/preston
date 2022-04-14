package bio.guoda.preston.util;

import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.store.VersionUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.concurrent.atomic.AtomicReference;

public class MostRecentVersionListener implements StatementListener {
    private final AtomicReference<IRI> mostRecent;

    public MostRecentVersionListener() {
        this.mostRecent = new AtomicReference<>();
    }

    @Override
    public void on(Quad statement) {
        IRI mostRecentCandidate = VersionUtil.mostRecentVersionForStatement(statement);
        if (mostRecentCandidate != null) {
            mostRecent.set(mostRecentCandidate);
        }
    }

    public IRI getMostRecent() {
        return mostRecent.get();
    }
}
