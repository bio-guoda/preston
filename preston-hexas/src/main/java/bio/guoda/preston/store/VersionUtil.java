package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.USED_BY;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.toStatement;

public class VersionUtil {

    public static IRI findMostRecentVersion(IRI provenanceRoot, HexaStore hexastore) throws IOException {
        return findMostRecentVersion(provenanceRoot, hexastore, null);
    }

    static IRI findMostRecentVersion(IRI provenanceRoot, HexaStoreReadOnly statementStore, StatementListener versionListener) throws IOException {
        IRI mostRecentVersion = findVersion(provenanceRoot, statementStore, versionListener);
        if (mostRecentVersion == null) {
            mostRecentVersion = findByPreviousVersion(provenanceRoot, statementStore, versionListener);
        }

        List<IRI> versions = new ArrayList<>();
        versions.add(mostRecentVersion);

        if (mostRecentVersion != null) {
            IRI lastVersionId = mostRecentVersion;
            IRI newerVersionId;

            while ((newerVersionId = findByPreviousVersion(lastVersionId, statementStore, versionListener)) != null) {
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

    private static IRI findByPreviousVersion(IRI versionSource, HexaStoreReadOnly statementStore, StatementListener versionListener) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(HAS_PREVIOUS_VERSION, versionSource));

        if (versionListener != null && mostRecentVersion != null) {
            versionListener.on(toStatement(mostRecentVersion, HAS_PREVIOUS_VERSION, versionSource));
        }
        return mostRecentVersion;
    }

    private static IRI findVersion(IRI provenanceRoot, HexaStoreReadOnly statementStore, StatementListener versionListener) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(provenanceRoot, HAS_VERSION));

        if (versionListener != null && mostRecentVersion != null) {
            versionListener.on(toStatement(provenanceRoot, HAS_VERSION, mostRecentVersion));
        }
        return mostRecentVersion;
    }

    public static IRI mostRecentVersionForStatement(Quad statement) {
        IRI mostRecentVersion = null;
        RDFTerm mostRecentTerm = null;
        if (statement.getPredicate().equals(HAS_PREVIOUS_VERSION)
                || statement.getPredicate().equals(WAS_DERIVED_FROM)
                || statement.getPredicate().equals(USED_BY)) {
            mostRecentTerm = statement.getSubject();
        } else if (statement.getPredicate().equals(HAS_VERSION)) {
            mostRecentTerm = statement.getObject();
        }

        if (mostRecentTerm instanceof IRI) {
            IRI mostRecentIRI = (IRI) mostRecentTerm;
            if (HashKeyUtil.isValidHashKey(mostRecentIRI)) {
                mostRecentVersion = mostRecentIRI;
            }
        }
        return mostRecentVersion;
    }
}
