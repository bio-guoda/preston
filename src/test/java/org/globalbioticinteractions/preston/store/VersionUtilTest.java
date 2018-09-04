package org.globalbioticinteractions.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.globalbioticinteractions.preston.RefNodeConstants;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;

import static org.globalbioticinteractions.preston.model.RefNodeFactory.toBlank;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.junit.Assert.assertThat;

public class VersionUtilTest {

    @Test
    public void getVersion() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        statementStore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), statementStore);

        assertThat(mostRecentVersion.toString(), Is.is("<http://some/version>"));
    }

    @Test
    public void versionPointingToItself() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        statementStore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), statementStore);

        assertThat(mostRecentVersion.toString(), Is.is("<http://some/version>"));
    }

    @Test
    public void versionPointingToItself2() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        statementStore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/other/version")), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), statementStore);

        assertThat(mostRecentVersion.toString(), Is.is("<http://some/other/version>"));
    }

}