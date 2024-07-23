package bio.guoda.preston.excel;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import bio.guoda.preston.store.Dereferencer;
import bio.guoda.preston.store.KeyValueStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.excel.XLSXHandler.rowsAsJsonStream;
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

        XLSXHandler.rowsAsJsonStream(out, resourceIRI, contentStore, 0, false);

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

        XLSXHandler.rowsAsJsonStream(out, resourceIRI, contentStore, 0, true);

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

        XLSXHandler.rowsAsJsonStream(out, resourceIRI, contentStore, 1, true);

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

        XLSXHandler.rowsAsJsonStream(out, resourceIRI, contentStore, 0, false);

        String expected = TestUtil.removeCarriageReturn(XLSXHandlerTest.class, "ictv.xlsx.json");
        String actual = new String(out.toByteArray(), StandardCharsets.UTF_8);

        JsonNode jsonNode = new ObjectMapper().readTree(StringUtils.split(actual, "\n")[0]);

        assertThat(StringUtils.replace(jsonNode.toString(), "\" :", "\":"),
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

        rowsAsJsonStream(out, resourceIRI, contentStore, 0, false);

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


        rowsAsJsonStream(out, resourceIRI, contentStore, 0, false);
    }

    @Test
    public void dumpImagesInTable() throws IOException {
        HashType hashType = HashType.sha256;
        InputStream resourceAsStream = getBMT111DemFile();
        IRI archiveContentId = Hasher.calcHashIRI(resourceAsStream, NullOutputStream.INSTANCE, hashType);

        List<Quad> statements = new ArrayList<>();
        StatementsListener listener = new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        };

        Dereferencer<InputStream> contentStore = new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getBMT111DemFile();
            }
        };

        XLSXHandler.emitPictureStatementsForXLSX(hashType, archiveContentId, listener, contentStore);


        StringBuilder stringBuilder = new StringBuilder();

        statements.forEach(statement -> {
            stringBuilder.append(statement.toString());
            stringBuilder.append("\n");
        });

        assertThat(stringBuilder.toString(), Is.is(
                "<zip:hash://sha256/1378ced58ad2ee0274e117da3b7b6f2fe56d6bda197408c55431680bd66b03f2!/xl/media/image1.png> <http://purl.org/dc/elements/1.1/format> \"image/png\" .\n" +
                        "<zip:hash://sha256/1378ced58ad2ee0274e117da3b7b6f2fe56d6bda197408c55431680bd66b03f2!/xl/media/image1.png> <http://purl.org/pav/hasVersion> <hash://sha256/fa89975444991fa4d181cbc61a32e1f32d4fd9fa69a41876c8ee6719fb7bb1dc> .\n" +
                        "<zip:hash://sha256/1378ced58ad2ee0274e117da3b7b6f2fe56d6bda197408c55431680bd66b03f2!/xl/media/image2.png> <http://purl.org/dc/elements/1.1/format> \"image/png\" .\n" +
                        "<zip:hash://sha256/1378ced58ad2ee0274e117da3b7b6f2fe56d6bda197408c55431680bd66b03f2!/xl/media/image2.png> <http://purl.org/pav/hasVersion> <hash://sha256/52714b537c23acbf9cf0234ed9b85dc84dade820d398aef53e57c3c62dcadbd1> .\n" +
                        "<zip:hash://sha256/1378ced58ad2ee0274e117da3b7b6f2fe56d6bda197408c55431680bd66b03f2!/xl/media/image3.png> <http://purl.org/dc/elements/1.1/format> \"image/png\" .\n" +
                        "<zip:hash://sha256/1378ced58ad2ee0274e117da3b7b6f2fe56d6bda197408c55431680bd66b03f2!/xl/media/image3.png> <http://purl.org/pav/hasVersion> <hash://sha256/4025ba7e5c8b450216393480192a18c78c261f5069dd4bb61210533cc08a7f49> .\n")
        );

    }

    @Test
    public void dumpImagesForDMT121InTable() throws IOException {
        IRI archiveContentId = Hasher.calcHashIRI(getBMT121DemFile(), NullOutputStream.INSTANCE, HashType.sha256);

        List<Quad> statements = new ArrayList<>();
        StatementsListener listener = new StatementsListenerAdapter() {

            @Override
            public void on(Quad statement) {
                statements.add(statement);
            }
        };

        Dereferencer<InputStream> contentStore = new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI uri) throws IOException {
                return getBMT111DemFile();
            }
        };

        XLSXHandler.emitPictureStatementsForXLSX(HashType.sha256, archiveContentId, listener, contentStore);


        StringBuilder stringBuilder = new StringBuilder();

        statements.forEach(statement -> {
            stringBuilder.append(statement.toString());
            stringBuilder.append("\n");
        });

        assertThat(stringBuilder.toString(), Is.is(
                "<zip:hash://sha256/91dabac3a4609d25d58595a693548b627d281a9a6efdbb3e03403c41da56bd80!/xl/media/image1.png> <http://purl.org/dc/elements/1.1/format> \"image/png\" .\n" +
                        "<zip:hash://sha256/91dabac3a4609d25d58595a693548b627d281a9a6efdbb3e03403c41da56bd80!/xl/media/image1.png> <http://purl.org/pav/hasVersion> <hash://sha256/fa89975444991fa4d181cbc61a32e1f32d4fd9fa69a41876c8ee6719fb7bb1dc> .\n" +
                        "<zip:hash://sha256/91dabac3a4609d25d58595a693548b627d281a9a6efdbb3e03403c41da56bd80!/xl/media/image2.png> <http://purl.org/dc/elements/1.1/format> \"image/png\" .\n" +
                        "<zip:hash://sha256/91dabac3a4609d25d58595a693548b627d281a9a6efdbb3e03403c41da56bd80!/xl/media/image2.png> <http://purl.org/pav/hasVersion> <hash://sha256/52714b537c23acbf9cf0234ed9b85dc84dade820d398aef53e57c3c62dcadbd1> .\n" +
                        "<zip:hash://sha256/91dabac3a4609d25d58595a693548b627d281a9a6efdbb3e03403c41da56bd80!/xl/media/image3.png> <http://purl.org/dc/elements/1.1/format> \"image/png\" .\n" +
                        "<zip:hash://sha256/91dabac3a4609d25d58595a693548b627d281a9a6efdbb3e03403c41da56bd80!/xl/media/image3.png> <http://purl.org/pav/hasVersion> <hash://sha256/4025ba7e5c8b450216393480192a18c78c261f5069dd4bb61210533cc08a7f49> .\n")
        );

    }

    private InputStream getBMT111DemFile() {
        return getClass().getResourceAsStream("Thomas_BMT111_demfile.xlsx");
    }

    private InputStream getBMT121DemFile() {
        return getClass().getResourceAsStream("Thomas_BMT121_demfile.xlsx");
    }


}
