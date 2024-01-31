package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Ignore;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class TaxoDrosFileExtractorTest {

    @Test
    public void streamTaxoDrosToLineJson() throws IOException {
        assertAssumptions("DROS5.TEXT.example.txt");

    }

    @Test
    public void streamTaxoDrosToLineJsonWithIncomplete() throws IOException {
        assertAssumptions("DROS5.TEXT.incomplete.txt");
    }

    @Test
    public void streamTaxoDrosToLineJsonWithDOI() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.doi.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.has("doi"), is(true));
        assertThat(taxonNode.get("doi").textValue(), is("10.7868/S0016675814060150"));
    }

    @Test
    public void streamTaxoDrosToLineJsonBook() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.book.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(StringUtils.startsWith(".Z.Nijhoff", ".Z"), is(true));

        assertThat(taxonNode.get("title").textValue(), is("Catalogue of the described Diptera from South Asia. 222 pp."));
        assertThat(taxonNode.get("type").textValue(), is("book"));
        assertThat(taxonNode.get("publisher").textValue(), is("Nijhoff"));
    }

    @Test
    public void streamTaxoDrosToLineJsonCollection() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.collection.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(StringUtils.startsWith(".Z.Nijhoff", ".Z"), is(true));

        assertThat(taxonNode.get("id").textValue(), is("collection, zmc"));
        assertThat(taxonNode.get("title").textValue(), is("Zoological Museum University of Copenhagen Universitetsparken 15 DK-2100 Copenhagen O Denmark"));
        assertThat(taxonNode.get("type").textValue(), is("collection"));
        assertThat(taxonNode.get("collection").textValue(), is("collection, zmc"));
    }

    @Test
    public void streamTaxoDrosToLineJsonWithDOILowerCase() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.doi.lower.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.has("doi"), is(true));
        assertThat(taxonNode.get("doi").textValue(), is("10.7868/S0016675814060150"));
    }

    @Test
    public void streamTaxoDrosToLineJsonWithMultilineFilename() throws IOException {
        assertAssumptions("DROS5.TEXT.longfilename.txt");
    }

    @Test
    public void streamTaxoDros3ToLineJson() throws IOException {
        String[] jsonObjects = getResource("DROS3.TEXT.example.txt");
        assertThat(jsonObjects.length, is(3));
        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L8"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("taxodros-dros3"));
        assertThat(taxonNode.get("id").asText(), is("abd el-halim et al., 2005"));
        assertThat(taxonNode.get("taxonId").asText(), is("histrioides"));
        JsonNode localities = taxonNode.get("localities");
        assertThat(localities, is(notNullValue()));
        assertThat(localities.isArray(), is(true));
        assertThat(localities.size(), is(2));
        assertThat(localities.get(0).asText(), is("kena"));
        assertThat(localities.get(1).asText(), is("sinai"));
        JsonNode keywords = taxonNode.get("keywords");
        assertThat(keywords, is(notNullValue()));
        assertThat(keywords.isArray(), is(true));
        assertThat(keywords.size(), is(3));
        assertThat(keywords.get(0).asText(), is("histrioides"));
        assertThat(keywords.get(1).asText(), is("distr$"));
        assertThat(keywords.get(2).asText(), is("egypt"));
    }

    @Test
    public void parseJournalInfoWithNumber() {
        ObjectNode ref = new ObjectMapper().createObjectNode();
        TaxoDrosFileStreamHandler.enrichWithJournalInfo(ref, "32(1981):107");

        assertThat(ref.get("pages").asText(), is("107"));
        assertThat(ref.get("volume").asText(), is("32"));
        assertThat(ref.get("number").asText(), is("1981"));
    }

    @Test
    public void parseJournalInfo2() {
        ObjectNode ref = new ObjectMapper().createObjectNode();
        TaxoDrosFileStreamHandler.enrichWithJournalInfo(ref, "32:107");

        assertThat(ref.get("pages").asText(), is("107"));
        assertThat(ref.get("volume").asText(), is("32"));
    }

    @Test
    public void parseJournalInfo3() {
        ObjectNode ref = new ObjectMapper().createObjectNode();
        TaxoDrosFileStreamHandler.enrichWithJournalInfo(ref, "35:351-362.");

        assertThat(ref.get("pages").asText(), is("351-362"));
        assertThat(ref.get("volume").asText(), is("35"));
    }

    private void assertAssumptions(String testResource) throws IOException {
        String[] jsonObjects = getResource(testResource);
        assertThat(jsonObjects.length, is(3));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L10"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("taxodros-dros5"));
        assertThat(taxonNode.get("id").asText(), is("abd el-halim et al., 2005"));
        assertThat(taxonNode.get("authors").asText(), is("Abd El-Halim, A.S., Mostafa, A.A., & Allam, K.A.M.a.,"));
        assertThat(taxonNode.get("title").asText(), is("Dipterous flies species and their densities in fourteen Egyptian governorates."));
        assertThat(taxonNode.get("journal").asText(), is("J. Egypt. Soc. Parasitol."));
        assertThat(taxonNode.get("volume").asText(), is("35"));
        assertThat(taxonNode.get("number"), is(nullValue()));
        assertThat(taxonNode.get("pages").asText(), is("351-362"));
        assertThat(taxonNode.get("year").asText(), is("2005"));
        assertThat(taxonNode.get("method").asText(), is("ocr"));
        assertThat(taxonNode.get("type").textValue(), is("article"));
        assertThat(taxonNode.get("filename").asText(), is("Abd El-Halim et al., 2005M.pdf"));
    }

    private String[] getResource(String testResource) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource(testResource);
                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                }
                return null;
            }
        };

        Quad statement = toStatement(
                toIRI("blip"),
                HAS_VERSION,
                toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1")
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        TaxoDrosFileExtractor extractor = new TaxoDrosFileExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        return StringUtils.split(actual, "\n");
    }


}
