package bio.guoda.preston.store;

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
import static org.hamcrest.MatcherAssert.assertThat;

public class StatementStoreImplTest {

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