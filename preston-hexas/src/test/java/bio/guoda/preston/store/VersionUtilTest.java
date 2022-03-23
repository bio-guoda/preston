package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.hamcrest.MatcherAssert.assertThat;

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

    @Ignore(value = "enable after implementing prov root selection")
    @Test
    public void versionPointingToItself2NonRoot() throws IOException {
        KeyValueStore testKeyValueStore = TestUtil.getTestPersistence();


        HexaStore hexastore = new HexaStoreImpl(testKeyValueStore, HashType.sha256);
        hexastore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/other/version")), toIRI("http://some/version"));
        hexastore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some/version"), hexastore);

        assertNotNull(mostRecentVersion);
        assertThat(mostRecentVersion.toString(), is("<http://some/other/version>"));
    }

    @Ignore(value = "enable after implementing prov root selection")
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


}