package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.BlobStoreReadOnly;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

public class DereferencerContentAddressedTarGZTest {

    @Test
    public void pickHashFromTarball() throws IOException {

        String knownPresentHash = "hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99";
        assertThat(knownPresentHash.length(), Is.is(78));

        try (InputStream is = getDerefTarGZ(null).dereference(RefNodeFactory.toIRI("tgz:https://example.com/preston-1a.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"))) {
            assertNotNull(is);
            IRI iri = Hasher.calcSHA256(is, new NullOutputStream(), false);
            assertThat(iri, Is.is(RefNodeFactory.toIRI(knownPresentHash)));
        }
    }

    @Test
    public void pickHashFromTarballAndCacheAll() throws IOException {
        String knownPresentHash = "hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99";
        assertThat(knownPresentHash.length(), Is.is(78));

        BlobStore testBlobStore = new BlobStoreAppendOnly(TestUtil.getTestPersistence(), false);
        assertNull(testBlobStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da")));

        try (InputStream is = getDerefTarGZ(testBlobStore).dereference(RefNodeFactory.toIRI("tgz:https://example.com/preston-1a.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"))) {
            assertNotNull(is);
            IRI iri = Hasher.calcSHA256(is, new NullOutputStream(), false);
            assertThat(iri, Is.is(RefNodeFactory.toIRI(knownPresentHash)));
        }

        assertNotNull(testBlobStore.get(RefNodeFactory.toIRI("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da")));

    }

    @Test
    public void missingHashExistingTar() throws IOException {
        assertNull(getDerefTarGZ(null).dereference(RefNodeFactory.toIRI("tgz:https://example.com/preston-1a.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a00")));
    }

    @Test
    public void existingHashMissingTar() throws IOException {
        assertNull(getDerefTarGZ(null).dereference(RefNodeFactory.toIRI("tgz:https://example.com/preston-2a.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a00")));
    }

    public DereferencerContentAddressedTarGZ getDerefTarGZ(BlobStore blobStore) {
        return new DereferencerContentAddressedTarGZ(new Dereferencer<InputStream>() {
            @Override
            public InputStream dereference(IRI uri) throws IOException {
                return getClass().getResourceAsStream("/preston-1a.tar.gz");
            }
        }, blobStore);
    }

}