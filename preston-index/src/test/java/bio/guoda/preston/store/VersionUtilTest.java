package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;

public class VersionUtilTest {

    @Test
    public void getVersion() throws IOException {
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();


        HexaStore hexastore = new HexaStoreImpl(testKeyValueStore, HashType.sha256);
        hexastore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), hexastore);

        assertThat(mostRecentVersion.toString(), is("<http://some/version>"));
    }

    @Test
    public void getTwoVersions() throws IOException {
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();

        HexaStore hexastore = new HexaStoreImpl(testKeyValueStore, HashType.sha256);
        hexastore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/later/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), hexastore);

        assertThat(mostRecentVersion.toString(), is("<http://some/later/version>"));
    }

    @Test
    public void versionPointingToItself() throws IOException {
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();


        HexaStore hexastore = new HexaStoreImpl(testKeyValueStore, HashType.sha256);
        hexastore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), hexastore);

        assertThat(mostRecentVersion.toString(), is("<http://some/version>"));
    }

    @Test
    public void versionPointingToItself2() throws IOException {
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();


        HexaStore hexastore = new HexaStoreImpl(testKeyValueStore, HashType.sha256);
        hexastore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/other/version")), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), hexastore);

        assertThat(mostRecentVersion.toString(), is("<http://some/other/version>"));
    }

    @Test
    public void historyOfSpecificNonRootVersion() throws IOException {
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();


        HexaStore hexastore = new HexaStoreImpl(testKeyValueStore, HashType.sha256);
        hexastore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some/version"), hexastore);

        assertNotNull(mostRecentVersion);
        assertThat(mostRecentVersion.toString(), is("<http://some/other/version>"));
    }

    @Test
    public void mostRecentForVersionStatement() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getNewer()),
                RefNodeConstants.HAS_PREVIOUS_VERSION,
                toIRI(getOlder())
        );

        IRI iri = VersionUtil.mostRecentVersion(provenanceStatement);

        assertThat(iri.getIRIString(), is(getNewer()));

    }

    @Test
    public void newerVersionStatementHasPreviousVersion() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getNewer()),
                RefNodeConstants.HAS_PREVIOUS_VERSION,
                toIRI(getOlder())
        );

        String someStatement = provenanceStatement.toString();

        IRI iri = VersionUtil.mostRecentVersion(someStatement);
        assertNotNull(iri);
        assertThat(iri.getIRIString(), is(getNewer()));

    }

    @Test
    public void newerVersionStatementUsedBy() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getNewer()),
                RefNodeConstants.USED_BY,
                toIRI(getOlder())
        );

        String someStatement = provenanceStatement.toString();

        IRI iri = VersionUtil.mostRecentVersion(someStatement);
        assertNotNull(iri);
        assertThat(iri.getIRIString(), is(getNewer()));

    }

    @Test
    public void newerVersionStatementDerivedFrom() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getNewer()),
                RefNodeConstants.WAS_DERIVED_FROM,
                toIRI(getOlder())
        );

        String someStatement = provenanceStatement.toString();

        IRI iri = VersionUtil.mostRecentVersion(someStatement);
        assertNotNull(iri);
        assertThat(iri.getIRIString(), is(getNewer()));

    }

    @Test
    public void newerVersionStatementHasVersion() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getOlder()),
                RefNodeConstants.HAS_VERSION,
                toIRI(getNewer())
        );

        String someStatement = provenanceStatement.toString();

        IRI iri = VersionUtil.mostRecentVersion(someStatement);
        assertNotNull(iri);
        assertThat(iri.getIRIString(), is(getNewer()));

    }


    @Test
    public void mostRecentForVersionStatement2() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getOlder()),
                RefNodeConstants.HAS_VERSION,
                toIRI(getNewer())
        );

        IRI iri = VersionUtil.mostRecentVersion(provenanceStatement);

        assertThat(iri.getIRIString(), is(getNewer()));

    }

    @Test
    public void mostRecentForVersionStatement3() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI(getNewer()),
                RefNodeConstants.WAS_DERIVED_FROM,
                toIRI(getOlder())
        );

        IRI iri = VersionUtil.mostRecentVersion(provenanceStatement);

        assertThat(iri.getIRIString(), is(getNewer()));

    }

    private String getOlder() {
        return "hash://sha256/f5851620a22110d6ebb73809df89c6321e79b4483dd2eb84ea77948505561463";
    }

    private String getNewer() {
        return "hash://sha256/77e30f34ca80fc7e2683e3953d0701a800862b2290d5617e8e5ef8230999e35f";
    }


}