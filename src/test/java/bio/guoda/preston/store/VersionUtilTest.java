package bio.guoda.preston.store;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import bio.guoda.preston.RefNodeConstants;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.Triple;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;

import static bio.guoda.preston.model.RefNodeFactory.toBlank;
import static bio.guoda.preston.model.RefNodeFactory.toDateTime;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class VersionUtilTest {

    @Test
    public void getVersion() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        statementStore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), statementStore);

        assertThat(mostRecentVersion.toString(), is("<http://some/version>"));
    }

    @Test
    public void versionPointingToItself() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        statementStore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), statementStore);

        assertThat(mostRecentVersion.toString(), is("<http://some/version>"));
    }

    @Test
    public void versionPointingToItself2() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        statementStore.put(Pair.of(toIRI("http://some"), RefNodeConstants.HAS_VERSION), toIRI("http://some/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/other/version")), toIRI("http://some/version"));
        statementStore.put(Pair.of(RefNodeConstants.HAS_PREVIOUS_VERSION, toIRI("http://some/version")), toIRI("http://some/other/version"));


        IRI mostRecentVersion = VersionUtil.findMostRecentVersion(toIRI("http://some"), statementStore);

        assertThat(mostRecentVersion.toString(), is("<http://some/other/version>"));
    }

    @Test
    public void generationTimeFor() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        BlobStore blobStore = new AppendOnlyBlobStore(testPersistence);

        Literal dateTime = VersionUtil.recordGenerationTimeFor(toIRI("http://some"), blobStore, statementStore, toDateTime("2018-10-25"));

        Triple triple = VersionUtil.generationTimeFor(toIRI("http://some"), statementStore, blobStore);

        assertNotNull(triple);
        assertThat(triple.getObject(), is(dateTime));
        assertThat(triple.toString(), is("<http://some> <http://www.w3.org/ns/prov#generatedAtTime> \"2018-10-25\"^^<http://www.w3.org/2001/XMLSchema#dateTime> ."));
    }

    @Test
    public void recordGenerationTime() throws IOException {
        Persistence testPersistence = TestUtil.getTestPersistence();


        StatementStore statementStore = new StatementStoreImpl(testPersistence);
        BlobStore blobStore = new AppendOnlyBlobStore(testPersistence);


        Literal dateTime = VersionUtil.recordGenerationTimeFor(toIRI("http://some"), blobStore, statementStore, toDateTime("2018-10-25"));

        assertNotNull(dateTime);
        assertThat(dateTime.toString(), is("\"2018-10-25\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));

    }

}