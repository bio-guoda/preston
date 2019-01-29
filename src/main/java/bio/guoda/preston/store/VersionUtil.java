package bio.guoda.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.model.RefNodeFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.GENERATED_AT_TIME;
import static bio.guoda.preston.RefNodeConstants.HAS_PREVIOUS_VERSION;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.model.RefNodeFactory.toDateTime;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;

public class VersionUtil {

    public static IRI findMostRecentVersion(IRI versionSource, StatementStore statementStore) throws IOException {
        return findMostRecentVersion(versionSource, statementStore, null);
    }

    public static IRI findMostRecentVersion(IRI versionSource, StatementStore statementStore, VersionListener versionListener) throws IOException {
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

    public static IRI findPreviousVersion(IRI versionSource, StatementStore statementStore, VersionListener versionListener) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(HAS_PREVIOUS_VERSION, versionSource));

        if (versionListener != null && mostRecentVersion != null) {
            versionListener.onVersion(toStatement(mostRecentVersion, HAS_PREVIOUS_VERSION, versionSource));
        }
        return mostRecentVersion;
    }

    public static IRI findVersion(IRI versionSource, StatementStore statementStore, VersionListener versionListener) throws IOException {
        IRI mostRecentVersion = statementStore.get(Pair.of(versionSource, HAS_VERSION));

        if (versionListener != null && mostRecentVersion != null) {
            versionListener.onVersion(toStatement(versionSource, HAS_VERSION, mostRecentVersion));
        }
        return mostRecentVersion;
    }

    public static Literal recordGenerationTimeFor(BlankNodeOrIRI derivedSubject, BlobStore blobStore, StatementStore statementStore) throws IOException {
        Literal nowLiteral = RefNodeFactory.nowDateTimeLiteral();
        return recordGenerationTimeFor(derivedSubject, blobStore, statementStore, nowLiteral);
    }

    public static Literal recordGenerationTimeFor(BlankNodeOrIRI derivedSubject, BlobStore blobStore, StatementStore statementStore, Literal dateTimeLiteral) throws IOException {
        String value = dateTimeLiteral.getLexicalForm();
        blobStore.putBlob(IOUtils.toInputStream(value, StandardCharsets.UTF_8));
        IRI value1 = Hasher.calcSHA256(value);
        statementStore.put(Pair.of(derivedSubject, GENERATED_AT_TIME), value1);
        return dateTimeLiteral;
    }

    public static Triple generationTimeFor(BlankNodeOrIRI subject, StatementStore statementStore, BlobStore blobStore) throws IOException {
        Triple statement1 = null;
        IRI timeKey = statementStore.get(Pair.of(subject, GENERATED_AT_TIME));
        if (timeKey != null) {
            InputStream input = blobStore.get(timeKey);
            if (input != null) {
                statement1 = toStatement(subject,
                        GENERATED_AT_TIME,
                        toDateTime(IOUtils.toString(input, StandardCharsets.UTF_8)));

            }
        }
        return statement1;
    }
}
