package bio.guoda.preston.excel;

import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.excel.XLSXHandler.asJsonStream;
import static org.hamcrest.MatcherAssert.assertThat;

public class XLSXHandlerTest {

    @Test
    public void dumpTable() throws IOException {
        // use this count to fetch all field information
        // if required
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("msw3-03.xlsx");
            }
        };

        XLSXHandler.asJsonStream(out, resourceIRI, contentStore, 0, false);

        String expected = TestUtil.removeCarriageReturn(XLSXHandlerTest.class, "msw3-03.xlsx.json");

        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);

        JsonNode jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[0]);

        assertThat(jsonNode.get("TAXON LEVEL").asText(), Is.is("ORDER"));

        jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[1]);

        assertThat(jsonNode.get("TAXON LEVEL").asText(), Is.is("FAMILY"));

        assertThat(actual, Is.is(expected));

    }
    @Test
    public void dumpTableHeaderless() throws IOException {
        // use this count to fetch all field information
        // if required
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("msw3-03.xlsx");
            }
        };

        XLSXHandler.asJsonStream(out, resourceIRI, contentStore, 0, true);

        String expected = TestUtil.removeCarriageReturn(XLSXHandlerTest.class, "msw3-03.xlsx.headerless.json");

        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);

        JsonNode jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[0]);

        assertThat(jsonNode.get("1").asText(), Is.is("ORDER"));

        assertThat(actual, Is.is(expected));

    }

    @Test
    public void dumpTableHeaderlessSkip() throws IOException {
        // use this count to fetch all field information
        // if required
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("msw3-03.xlsx");
            }
        };

        XLSXHandler.asJsonStream(out, resourceIRI, contentStore, 1, true);

        String expected = TestUtil.removeCarriageReturn(XLSXHandlerTest.class, "msw3-03.xlsx.headerless.skip.json");

        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);

        JsonNode jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[0]);

        assertThat(jsonNode.get("1").asText(), Is.is("MONOTREMATA"));

        assertThat(actual, Is.is(expected));

    }

    @Test
    public void dumpTableICTV() throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        IRI resourceIRI = RefNodeFactory.toIRI("some:iri");

        KeyValueStoreReadOnly contentStore = new KeyValueStoreReadOnly() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getClass().getResourceAsStream("ictv.xlsx");
            }
        };

        XLSXHandler.asJsonStream(out, resourceIRI, contentStore, 0, false);

        String expected = TestUtil.removeCarriageReturn(XLSXHandlerTest.class, "ictv.xlsx.json");
        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);

        JsonNode jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[0]);

        assertThat(StringUtils.replace(jsonNode.toPrettyString(), "\" :", "\":"),
                Is.is(expected));

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

        asJsonStream(out, resourceIRI, contentStore, 0, false);

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


        asJsonStream(out, resourceIRI, contentStore, 0, false);
    }


}
