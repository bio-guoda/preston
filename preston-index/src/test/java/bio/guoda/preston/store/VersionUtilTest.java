package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.util.UUIDUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
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
    public void getVersionWithCut() throws IOException {

        Quad versionStatement = RefNodeFactory.toStatement(toIRI("http://some"), RefNodeConstants.HAS_VERSION, toIRI("cut:hash://sha256/a54fbd3bc1eba272cdba5ba4f4c121c1ee45eee62252f8b1a7afda75c1545c7c!/b2987-7188"));
        IRI mostRecentVersion = VersionUtil.mostRecentVersion(versionStatement);

        assertThat(mostRecentVersion.toString(), is("<cut:hash://sha256/a54fbd3bc1eba272cdba5ba4f4c121c1ee45eee62252f8b1a7afda75c1545c7c!/b2987-7188>"));
    }

    @Test
    public void getVersionWithCutFromString() throws IOException {

        Quad versionStatement = RefNodeFactory.toStatement(toIRI("cut:hash://sha256/a54fbd3bc1eba272cdba5ba4f4c121c1ee45eee62252f8b1a7afda75c1545c7c!/b2987-7188"), RefNodeConstants.HAS_VERSION, toIRI("cut:hash://sha256/a54fbd3bc1eba272cdba5ba4f4c121c1ee45eee62252f8b1a7afda75c1545c7c!/b2987-7188"));
        IRI mostRecentVersion = VersionUtil.mostRecentVersion(versionStatement.toString());

        assertThat(mostRecentVersion.toString(), is("<cut:hash://sha256/a54fbd3bc1eba272cdba5ba4f4c121c1ee45eee62252f8b1a7afda75c1545c7c!/b2987-7188>"));
    }

    @Test
    public void getVersionWithCutFromStringWithGraphName() throws IOException {

        IRI mostRecentVersion = VersionUtil.mostRecentVersion(
                "<https://example.org>" +
                        " <http://purl.org/pav/hasVersion>" +
                        " <cut:hash://sha1/398ab74e3da160d52705bb2477eb0f2f2cde5f15!/b1-2>" +
                        " .");

        assertThat(mostRecentVersion.toString(), is("<cut:hash://sha1/398ab74e3da160d52705bb2477eb0f2f2cde5f15!/b1-2>"));
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
    public void getMostVersionWithMalformedStatement() throws IOException {

        IRI mostRecentVersion = VersionUtil.mostRecentVersion("<https://archive.org/download/studyofleaves00denn/studyofleaves00denn_djvu.txt> <http://purl.org/pav/hasVersion> <hash://sha256/8aaed7911f9fc<urn:uuid:cec9d97d-d0d0-47d3-8367-dfbb41e31ecf> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#Generation> <urn:uuid:cec9d97d-d0d0-47d3-8367-dfbb41e31ecf> .");

        assertThat(mostRecentVersion, is(nullValue()));
    }


    @Test
    public void newerVersionStatementDerivedFrom() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI("foo:bar"),
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
    public void mostRecentVersion() {
        IRI iri = VersionUtil.mostRecentVersion("<foo:bar> <http://purl.org/pav/hasVersion> <foo:bar:1> .");
        assertThat(iri, is(notNullValue()));
        assertThat(iri.getIRIString(), is("foo:bar:1"));
    }


    @Test
    public void mostRecentVersionWithNamespace() {
        IRI iri = VersionUtil.mostRecentVersion("<foo:bar> <http://purl.org/pav/hasVersion> <foo:bar:1> <ns:name> .");
        assertThat(iri, is(notNullValue()));
        assertThat(iri.getIRIString(), is("foo:bar:1"));

    }

    @Test
    public void newerVersionStatementHasVersion() {
        Quad provenanceStatement = RefNodeFactory.toStatement(
                toIRI("foo:bar"),
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