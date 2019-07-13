package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class VersionUtil {

    public static IRI findMostRecentVersion(IRI versionSource, StatementStore statementStore) throws IOException {
        return findMostRecentVersion(versionSource, statementStore, null);
    }

    public static IRI findMostRecentVersion(IRI versionSource, StatementStoreReadOnly statementStore, VersionListener versionListener) throws IOException {
        IRI mostRecentVersion = findVersion(versionSource, statementStore, versionListener);

        List<IRI> versions = new ArrayList<>();
        versions.add(mostRecentVersion);

        if (mostRecentVersion != null) {
            IRI lastVersionId = mostRecentVersion;
            IRI newerVersionId;

            while ((newerVersionId = findPreviousVersion(lastVersionId, statementStore, versionListener)) != null) {
                versions.add(mostRecentVersion);
                if (versions.contains(newerVersionId)) {
                    break;
                } else {
                    versions.add(newerVersionId);
                }
                lastVersionId = newerVersionId;
            }
            mostRecentVersion = lastVersionId;
        }

        return mostRecentVersion;
    }

    public static IRI findPreviousVersion(IRI versionSource, StatementStoreReadOnly statementStore, VersionListener versionListener) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(HAS_PREVIOUS_VERSION, versionSource));

        if (versionListener != null && mostRecentVersion != null) {
            versionListener.onVersion(toStatement(mostRecentVersion, HAS_PREVIOUS_VERSION, versionSource));
        }
        return mostRecentVersion;
    }

    public static IRI findVersion(IRI versionSource, StatementStoreReadOnly statementStore, VersionListener versionListener) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(versionSource, HAS_VERSION));

        if (versionListener != null && mostRecentVersion != null) {
            versionListener.onVersion(toStatement(versionSource, HAS_VERSION, mostRecentVersion));
        }
        return mostRecentVersion;
    }

    public static IRI mostRecentVersionForStatement(Triple statement) {
        IRI mostRecentVersion = null;
        RDFTerm mostRecentTerm = null;
        if (statement.getPredicate().equals(HAS_PREVIOUS_VERSION)) {
            mostRecentTerm = statement.getSubject();
        } else if (statement.getPredicate().equals(HAS_VERSION)) {
            mostRecentTerm = statement.getObject();
        }
        if (mostRecentTerm instanceof IRI) {
            IRI mostRecentIRI = (IRI) mostRecentTerm;
            if (!RefNodeFactory.isBlankOrSkolemizedBlank(mostRecentIRI)) {
                mostRecentVersion = mostRecentIRI;
            }
        }
        return mostRecentVersion;
    }
}
