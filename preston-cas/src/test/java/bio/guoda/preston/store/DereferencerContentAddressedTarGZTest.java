package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;

public class DereferencerContentAddressedTarGZTest {

    @Test
    public void extractSHA256URI() {
        IRI iri = DereferencerContentAddressedTarGZ.extractHashURI("tgz:https://example.com/preston-a1.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99");
        assertThat(iri.getIRIString(), Is.is("hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"));
    }

    @Test
    public void extractSHA1URI() {
        IRI iri = DereferencerContentAddressedTarGZ.extractHashURI("tgz:https://example.com/data.zip!/04/75/047595d0fae972fbed0c51b4a41c7a349e0c47bb");
        assertThat(iri.getIRIString(), Is.is("hash://sha1/047595d0fae972fbed0c51b4a41c7a349e0c47bb"));
    }

    @Test
    public void extractMD5URI() {
        IRI iri = DereferencerContentAddressedTarGZ.extractHashURI("zip:https://example.com/data.zip!/27/f5/27f552c25bc733d05a5cc67e9ba63850");
        assertThat(iri.getIRIString(), Is.is("hash://md5/27f552c25bc733d05a5cc67e9ba63850"));
    }

    @Test
    public void pickHashFromTarball() throws IOException {
        String knownPresentHash = "hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99";
        assertThat(knownPresentHash.length(), Is.is(78));

        try (InputStream is = getDerefTarGZ(null).get(RefNodeFactory.toIRI("tgz:https://example.com/preston-a1.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"))) {
            assertNotNull(is);
            IRI iri = Hasher.calcHashIRI(is, NullOutputStream.INSTANCE, false, HashType.sha256);
            assertThat(iri, Is.is(RefNodeFactory.toIRI(knownPresentHash)));
        }
    }

    @Test
    public void pickHashFromTarballAndCacheAll() throws IOException {
        String knownPresentHash = "hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99";
        assertThat(knownPresentHash.length(), Is.is(78));

        BlobStore testBlobStore = new BlobStoreAppendOnly(TestUtil.getTestPersistenceWithRemove(), false, HashType.sha256);
        assertNull(testBlobStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da")));

        try (InputStream is = getDerefTarGZ(testBlobStore).get(RefNodeFactory.toIRI("tgz:https://example.com/preston-a1.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"))) {
            assertNotNull(is);
            IRI iri = Hasher.calcHashIRI(is, NullOutputStream.INSTANCE, false, HashType.sha256);
            assertThat(iri, Is.is(RefNodeFactory.toIRI(knownPresentHash)));
        }

        assertNotNull(testBlobStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da")));

    }

    @Test
    public void missingHashExistingTar() throws IOException {
        assertNull(getDerefTarGZ(null).get(RefNodeFactory.toIRI("tgz:https://example.com/preston-a1.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a00")));
    }

    @Test
    public void existingHashMissingTar() throws IOException {
        assertNull(getDerefTarGZ(null).get(RefNodeFactory.toIRI("tgz:https://example.com/preston-2a.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a00")));
    }

    public DereferencerContentAddressedTarGZ getDerefTarGZ(BlobStore blobStore) {
        return new DereferencerContentAddressedTarGZ(new Dereferencer<InputStream>() {
            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("/preston-a1.tar.gz");
            }
        }, blobStore);
    }

}