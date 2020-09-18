package bio.guoda.preston.store;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.model.RefNodeFactory;
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
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class StatementStoreImplTest {

    @Test
    public void calculateKey() {
        IRI publisher = RefNodeFactory.toIRI("https://search.idigbio.org/v2/search/publishers");
        IRI version = RefNodeFactory.toIRI("http://purl.org/pav/hasVersion");

        assertThat(StatementStoreImpl.calculateHashFor(publisher).getIRIString(),
                is("hash://sha256/3edfe376ce9a6602fec3a6d3fa30d1d97bbf7a768fb855c8c75eeab389e1e3ef"));

        assertThat(StatementStoreImpl.calculateHashFor(version).getIRIString(),
                is("hash://sha256/0b658d6c9e2f6275fee7c564a229798c56031c020ded04c1040e30d2527f1806"));

        IRI iri = StatementStoreImpl.calculateKeyFor(Pair.of(publisher, version));
        assertThat(iri.getIRIString(),
                is("hash://sha256/a21d81acb039ca8daa013b4eebe52d5eda4f23d29c95d0f04888583ca5c8af4e"));
    }

    @Test
    public void calculateRootArchiveQueryKey() {
        IRI iri = StatementStoreImpl.calculateKeyFor(Pair.of(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH, RefNodeConstants.HAS_VERSION));
        assertThat(iri.getIRIString(),
                is("hash://sha256/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a"));
    }

    @Test
    public void calculateRootURN_UUID_ArchiveQueryKey() {
        IRI iri = StatementStoreImpl.calculateKeyFor(Pair.of(RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_URN_UUID, RefNodeConstants.HAS_VERSION));
        assertThat(iri.getIRIString(),
                is("hash://sha256/7fd737aed8f0d9acefb35db505cd77e81157dbf23c51203db53b00dea68af467"));
    }

    @Test
    public void putKey() throws IOException {
        StatementStore statementStore = new StatementStoreImpl(new KeyValueStore() {

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
        });

        Pair<RDFTerm, RDFTerm> queryKey = Pair.of(RefNodeFactory.toIRI("bla"), RefNodeFactory.toIRI("boo"));
        statementStore.put(queryKey, RefNodeFactory.toIRI(""));

        IRI iri1 = statementStore.get(queryKey);

        assertThat(iri1, is(nullValue()));

        statementStore.put(queryKey, RefNodeFactory.toIRI("foo"));

        iri1 = statementStore.get(queryKey);

        assertThat(iri1.getIRIString(), is("foo"));

    }

}