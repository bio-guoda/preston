package bio.guoda.preston.dbase;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.dbase.DBaseHandler.asJsonStream;
import static org.hamcrest.MatcherAssert.assertThat;

public class DBaseHandlerTest {

    @Test
    public void dumpTable() throws IOException {
        // use this count to fetch all field information
        // if required
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI( "some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("CMMUS2K.DBF");
            }
        };


        asJsonStream(out, resourceIRI, contentStore);

        String expected = IOUtils.toString(TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream("CMMUS2K.json")), StandardCharsets.UTF_8);
        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);
        assertThat(actual, Is.is(expected));
    }

    @Test
    public void nonDBFFile() throws IOException {
        assertNonDBFResource("notDBF.txt");
    }

    @Test
    public void nonDBFFile2() throws IOException {
        assertNonDBFResource("nonDBF2.DS_Store");
    }

    private void assertNonDBFResource(String nonDBFResource) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI( "some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream(nonDBFResource);
            }
        };


        asJsonStream(out, resourceIRI, contentStore);

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


        asJsonStream(out, resourceIRI, contentStore);
    }

}
