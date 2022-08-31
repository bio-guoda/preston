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
    public void putWellFormedSHA256Key() throws IOException {
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

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));

        hexastore.put(queryKey, RefNodeFactory.toIRI("hash://sha256/853ff93762a06ddbf722c4ebe9ddd66d8f63ddaea97f521c3ecc20da7c976020"));

        IRI iri1 = hexastore.get(queryKey);

        assertThat(iri1.getIRIString(), is("hash://sha256/853ff93762a06ddbf722c4ebe9ddd66d8f63ddaea97f521c3ecc20da7c976020"));

    }

    @Test
    public void putWellFormedMD5Key() throws IOException {
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
        }, HashType.md5);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));

        hexastore.put(queryKey, RefNodeFactory.toIRI("hash://md5/22c3683b094136c3398391ae71b20f04"));

        IRI iri1 = hexastore.get(queryKey);

        assertThat(iri1.getIRIString(), is("hash://md5/22c3683b094136c3398391ae71b20f04"));

    }

    @Test(expected = IOException.class)
    public void putMisalignedKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                fail("should not put malformed key");
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                fail("should not put malformed key");
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return null;
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));

        hexastore.put(queryKey, RefNodeFactory.toIRI("hash://md5/22c3683b094136c3398391ae71b20f04"));
    }

    @Test(expected = IOException.class)
    public void putEmptyKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                fail("should not put malformed key");
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {
                fail("should not put malformed key");
            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return null;
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));

        hexastore.put(queryKey, RefNodeFactory.toIRI(""));
    }

    @Test(expected = IOException.class)
    public void failedToGetKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                throw new IOException("kaboom!");
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        hexastore.get(queryKey);

    }

    @Test(expected = IOException.class)
    public void getEmptyKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return IOUtils.toInputStream("", StandardCharsets.UTF_8);
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        hexastore.get(queryKey);

    }

    @Test(expected = IOException.class)
    public void getMalformedKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return IOUtils.toInputStream("https://example.org/foo", StandardCharsets.UTF_8);
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        hexastore.get(queryKey);

    }

    @Test
    public void getWellFormedSHA256Key() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return IOUtils.toInputStream("hash://sha256/853ff93762a06ddbf722c4ebe9ddd66d8f63ddaea97f521c3ecc20da7c976020", StandardCharsets.UTF_8);
            }
        }, HashType.sha256);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        hexastore.get(queryKey);

    }

    @Test
    public void getWellFormedMD5Key() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return IOUtils.toInputStream("hash://md5/22c3683b094136c3398391ae71b20f04", StandardCharsets.UTF_8);
            }
        }, HashType.md5);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        hexastore.get(queryKey);

    }

    @Test(expected = IOException.class)
    public void getMismatchedFormedKey() throws IOException {
        HexaStore hexastore = new HexaStoreImpl(new KeyValueStore() {

            @Override
            public IRI put(KeyGeneratingStream keyGeneratingStream, InputStream is) throws IOException {
                return null;
            }

            @Override
            public void put(IRI key, InputStream is) throws IOException {

            }

            @Override
            public InputStream get(IRI key) throws IOException {
                return IOUtils.toInputStream("hash://sha256/853ff93762a06ddbf722c4ebe9ddd66d8f63ddaea97f521c3ecc20da7c976020", StandardCharsets.UTF_8);
            }
        }, HashType.md5);

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        hexastore.get(queryKey);

    }

}