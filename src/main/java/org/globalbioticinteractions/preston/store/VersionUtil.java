package org.globalbioticinteractions.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;

public class VersionUtil {

    public static IRI findMostRecentVersion(IRI versionSource, VersionListener versionListener, StatementStore statementStore) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(versionSource, HAS_VERSION));

        if (mostRecentVersion != null) {
            versionListener.onVersion(toStatement(versionSource, HAS_VERSION, mostRecentVersion));
            mostRecentVersion = findLastVersion(mostRecentVersion, versionListener, statementStore);
        }
        return mostRecentVersion;
    }

    private static IRI findLastVersion(IRI existingId, VersionListener versionListener, StatementStore statementStore) throws IOException {
        IRI lastVersionId = existingId;
        IRI newerVersionId;
        while ((newerVersionId = statementStore.get(Pair.of(HAS_PREVIOUS_VERSION, lastVersionId))) != null) {
            versionListener.onVersion(toStatement(newerVersionId, HAS_PREVIOUS_VERSION, lastVersionId));
            lastVersionId = newerVersionId;
        }
        return lastVersionId;
    }
}
