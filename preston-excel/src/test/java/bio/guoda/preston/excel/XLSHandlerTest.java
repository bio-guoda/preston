package bio.guoda.preston.excel;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.excel.XLSHandler.asJsonStream;
import static org.hamcrest.MatcherAssert.assertThat;

public class XLSHandlerTest {

    @Test
    public void dumpTable() throws IOException {
        // use this count to fetch all field information
        // if required
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("msw3-03.xls");
            }
        };


        XLSHandler.asJsonStream(out, resourceIRI, contentStore);

        String expected = TestUtil.removeCarriageReturn(XLSHandlerTest.class, "msw3-03.xls.json");

        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);

        JsonNode jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[1]);

        assertThat(jsonNode.get("TAXON LEVEL").asText(), Is.is("FAMILY"));

        assertThat(actual, Is.is(expected));
    }


    @Test
    public void nonXLS() throws IOException {
        assertNonExcelResource("notXLS.txt");
    }

    private void assertNonExcelResource(String nonDBFResource) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

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
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                throw new IOException("kaboom!");
            }
        };


        asJsonStream(out, resourceIRI, contentStore);
    }


}
