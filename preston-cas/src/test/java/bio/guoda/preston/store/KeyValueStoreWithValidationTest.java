package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

public class KeyValueStoreWithValidationTest {

    @Test(expected = IOException.class)
    public void putInvalid() throws IOException {
        KeyValueStoreWithRemove staging = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore verified = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore backing = TestUtil.getTestPersistenceWithRemove();


        KeyValueStore keyStore = new KeyValueStoreWithValidation(
                new ValidatingKeyValueStreamHashTypeIRIFactory(),
                staging,
                verified,
                backing
        );

        try {
            keyStore.put(RefNodeFactory.toIRI("foo:bar"), IOUtils.toInputStream("hello", StandardCharsets.UTF_8));
        } catch (IOException ex) {
            assertThat(ex.getMessage(), Is.is("found unsupported or invalid hash key [<foo:bar>]")
            );
            throw ex;
        }
    }


    @Test
    public void putValid() throws IOException {
        KeyValueStoreWithRemove staging = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore verified = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore backing = TestUtil.getTestPersistenceWithRemove();

        KeyValueStore keyStore = new KeyValueStoreWithValidation(
                new ValidatingKeyValueStreamHashTypeIRIFactory(),
                staging,
                verified,
                backing);

        IRI validKey = RefNodeFactory.toIRI("hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f");
        assertNull(staging.get(validKey));

        IRI validResult = RefNodeFactory.toIRI("hash://sha256/98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4");
        keyStore.put(
                validKey,
                IOUtils.toInputStream(validResult.getIRIString(), StandardCharsets.UTF_8)
        );

        InputStream inputStream = verified.get(validKey);
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is(validResult.getIRIString()));

        assertNull(staging.get(validKey));
    }

    @Test(expected = IOException.class)
    public void getInvalid() throws IOException {
        KeyValueStoreWithRemove staging = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore verified = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore backing = TestUtil.getTestPersistenceWithRemove();

        KeyValueStore keyStore = new KeyValueStoreWithValidation(
                new ValidatingKeyValueStreamHashTypeIRIFactory(),
                staging,
                verified,
                backing);

        IRI validKey = RefNodeFactory.toIRI("hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f");
        String invalidResult = "hello";
        backing.put(
                validKey,
                IOUtils.toInputStream(invalidResult, StandardCharsets.UTF_8)
        );

        try {
            keyStore.get(validKey);
        } catch (IOException ex) {
            assertThat(ex.getMessage(), Is.is(
                    "invalid results received for query [hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f] because [invalid key length: expected results for query [hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f] to be [78] long, but got result with length [5] instead" +
                            ", and because" +
                            " invalid key pattern: expected results for query key [hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f] to match pattern [hash://sha256/([a-fA-F0-9]){64}]]")
            );
            throw ex;
        }
    }

    @Test
    public void getValid() throws IOException {
        KeyValueStoreWithRemove staging = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore verified = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore backing = TestUtil.getTestPersistenceWithRemove();

        KeyValueStore keyStore = new KeyValueStoreWithValidation(
                new ValidatingKeyValueStreamHashTypeIRIFactory(),
                staging,
                verified,
                backing);

        IRI validKey = RefNodeFactory.toIRI("hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f");
        IRI validResult = RefNodeFactory.toIRI("hash://sha256/98ea6e4f216f2fb4b69fff9b3a44842c38686ca685f3f55dc48c5d3fb1107be4");

        backing.put(
                validKey,
                IOUtils.toInputStream(validResult.getIRIString(), StandardCharsets.UTF_8)
        );

        assertNull(staging.get(validKey));
        InputStream inputStream = keyStore.get(validKey);
        assertThat(IOUtils.toString(inputStream, StandardCharsets.UTF_8), Is.is(validResult.getIRIString()));

        assertNull(staging.get(validKey));
    }

    @Test
    public void getUnknownKey() throws IOException {
        KeyValueStoreWithRemove staging = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore verified = TestUtil.getTestPersistenceWithRemove();
        KeyValueStore backing = TestUtil.getTestPersistenceWithRemove();

        KeyValueStore keyStore = new KeyValueStoreWithValidation(
                new ValidatingKeyValueStreamHashTypeIRIFactory(),
                staging,
                verified,
                backing);

        IRI validKey = RefNodeFactory.toIRI("hash://sha256/00e3261a6e0d79c329445acd540fb2b07187a0dcf6017065c8814010283ac67f");

        assertNull(keyStore.get(validKey));
    }

}