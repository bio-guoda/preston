package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;

public class DereferencerContentAddressedTarGZTest {

    @Test
    public void pullThroughArchive() throws IOException {

        KeyValueStore testPersistence = TestUtil.getTestPersistence();
        BlobStore blobStore = new BlobStoreAppendOnly(testPersistence, false);

        DereferencerContentAddressedTarGZ derefTarGZ = new DereferencerContentAddressedTarGZ(new Dereferencer<InputStream>() {
            @Override
            public InputStream dereference(IRI uri) throws IOException {
                return getClass().getResourceAsStream("/preston-a1.tar.gz");
            }
        }, blobStore);

        String hashKey = "hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99";
        assertThat(hashKey.length(), Is.is(78));

        IRI dereference = derefTarGZ.dereference(RefNodeFactory.toIRI("tgz:https://example.com/preston-a1.tar.gz!/1a/57/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"));

        assertThat(dereference, Is.is(RefNodeFactory.toIRI("hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99")));

        assertNotNull(testPersistence.get("hash://sha256/a12dd6335e7803027da3007e26926c5c946fea9803a5eb07908d978998d933da"));
        assertNotNull(testPersistence.get("hash://sha256/1a57e55a780b86cff38697cf1b857751ab7b389973d35113564fe5a9a58d6a99"));
    }

}