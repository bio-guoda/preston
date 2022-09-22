package bio.guoda.preston.store;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.fail;

public class HexaStoreImplTest {

    @Test
    public void putKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            Map<String, String> keyMap = new HashMap<String, String>();

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                String s = IOUtils.toString(is, StandardCharsets.UTF_8);
                if (StringUtils.isBlank(s)) {
                    throw new IOException("no empty expected here");
                }
                keyMap.put(key.getIRIString(), s);
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                String s = keyMap.get(key.getIRIString());
                return StringUtils.isBlank(s) ? null : IOUtils.toInputStream(s, StandardCharsets.UTF_8);
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(
                RefNodeFactory.toIRI("bla"),
                RefNodeFactory.toIRI("boo")
        );

        String someSha256HashIRI = "hash://sha256/ab3d07f3169ccbd0ed6c4b45de21519f9f938c72d24124998aab949ce83bb51b";
        hexastore.put(queryKey, RefNodeFactory.toIRI(someSha256HashIRI));

        IRI iriFound = hexastore.get(queryKey);

        assertThat(iriFound.getIRIString(), is(someSha256HashIRI));

    }

    @Test(expected = IOException.class)
    public void getNonExistingKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            Map<String, String> keyMap = new HashMap<>();

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                String s = IOUtils.toString(is, StandardCharsets.UTF_8);
                if (StringUtils.isBlank(s)) {
                    throw new IOException("no empty expected here");
                }
                keyMap.put(key.getIRIString(), s);
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                String s = keyMap.get(key.getIRIString());
                return StringUtils.isBlank(s) ? null : IOUtils.toInputStream(s, StandardCharsets.UTF_8);
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(
                RefNodeFactory.toIRI("bla"),
                RefNodeFactory.toIRI("boo")
        );

        hexastore.get(queryKey);

    }

    @Test(expected = IOException.class)
    public void getKeyForFailingStore() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            Map<String, String> keyMap = new HashMap<>();

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                String s = IOUtils.toString(is, StandardCharsets.UTF_8);
                if (StringUtils.isBlank(s)) {
                    throw new IOException("no empty expected here");
                }
                keyMap.put(key.getIRIString(), s);
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("kaboom!");
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(
                RefNodeFactory.toIRI("bla"),
                RefNodeFactory.toIRI("boo")
        );

        IRI iri1 = hexastore.get(queryKey);

        assertThat(iri1, is(nullValue()));

        hexastore.get(queryKey);
    }

    @Test(expected = IOException.class)
    public void putInvalidKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                //
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                fail();
                throw new IOException("boom!");
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(
                RefNodeFactory.toIRI("bla"),
                RefNodeFactory.toIRI("boo")
        );
        hexastore.put(queryKey, RefNodeFactory.toIRI(""));
    }

}