package bio.guoda.preston.paradox;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.paradox.ParadoxHandler.asJsonStream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertNotNull;

public class ParadoxHandlerTest {

    @Test
    public void dumpTable() throws IOException {
        // use this count to fetch all field information
        // if required
        IRI resourceIRI = RefNodeFactory.toIRI( "some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("COLL.DB");
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        asJsonStream(out, resourceIRI, "COLL.DB", contentStore);

        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        String expected = TestUtil.removeCarriageReturn(ParadoxHandlerTest.class, "COLL.DB.json");

        assertThat(actual, Is.is(expected));
    }

    @Test
    public void dumpSpeciesDB() throws IOException {
        // use this count to fetch all field information
        // if required
        IRI resourceIRI = RefNodeFactory.toIRI( "some:iri");

        InputStream resourceAsStream = getClass().getResourceAsStream("SPECIES.DB");

        assertNotNull(resourceAsStream);

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return resourceAsStream;
            }
        };

        ByteArrayOutputStream out = new ByteArrayOutputStream();

        asJsonStream(out, resourceIRI, "SPECIES.DB", contentStore);

        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        String expected = TestUtil.removeCarriageReturn(ParadoxHandlerTest.class, "SPECIES.DB.json");

        assertThat(actual, Is.is(expected));

        // https://github.com/bio-guoda/preston/issues/237
        assertThat(actual, containsString("BLOB_CLOB_TYPES_NOT_SUPPORTED"));
    }


    @Test
    public void nonDB() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI( "some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("not-a.DB");
            }
        };


        asJsonStream(out, resourceIRI, "table", contentStore);

        assertThat(out.size(), Is.is(0));
    }

    @Test(expected = IOException.class)
    public void failedToRetrieve() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI( "some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        };


        asJsonStream(out, resourceIRI, "table", contentStore);
    }

}
