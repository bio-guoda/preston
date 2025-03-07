package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ContentStreamException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
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
import java.util.Arrays;
import java.util.Properties;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class TaxoDrosFileExtractorTest {

    public static final String NON_JOURNAL_TITLE = "title";

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

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        JsonNode first = taxonNode.at("/related_identifiers/0");
        assertThat(first.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(first.get("identifier").asText(), is("urn:lsid:taxodros.uzh.ch:id:abd%20el-halim%20et%20al.%2C%202005"));
        JsonNode second = taxonNode.at("/related_identifiers/1");
        assertThat(second.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(second.get("identifier").asText(), is("10.7868/S0016675814060150"));
    }

    @Test
    public void streamTaxoDrosToLineJsonYear() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.year.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        JsonNode first = taxonNode.at("/related_identifiers/0");
        assertThat(first.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(first.get("identifier").asText(), is("urn:lsid:taxodros.uzh.ch:id:huang%20%26%20chen%2C%202016"));
        JsonNode second = taxonNode.at("/related_identifiers/1");
        assertThat(second.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(second.get("identifier").asText(), is("10.11646/zootaxa.4161.2.4"));


        assertThat(taxonNode.get("publication_date").textValue(), is("2016"));
    }

    @Test
    public void authorsPeriodInsteadOfComma() throws IOException {
        // note that ".A Ward. P.I.," should be ".A Ward, P.I.," (notice the Ward, instead of Ward.)
        String[] jsonObjects = getResource("DROS5.TEXT.ward2002.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        JsonNode first = taxonNode.at("/related_identifiers/0");
        assertThat(first.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(first.get("identifier").asText(), is("urn:lsid:taxodros.uzh.ch:id:ward%2C%202002"));

        assertThat(taxonNode.get("creators").size(), is(0));
    }

    @Test
    public void authorsPeriodInsteadOfCommaFixed() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.ward2002-fixed.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        JsonNode first = taxonNode.at("/related_identifiers/0");
        assertThat(first.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(first.get("identifier").asText(), is("urn:lsid:taxodros.uzh.ch:id:ward%2C%202002"));

        assertThat(taxonNode.get("creators").size(), is(greaterThan(0)));
    }

    @Test
    public void streamTaxoDrosToLineJsonAuthorsWithAmpersand() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.authors.ampersand.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);


        JsonNode first = taxonNode.at("/related_identifiers/0");
        assertThat(first.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(first.get("identifier").asText(), is("urn:lsid:taxodros.uzh.ch:id:abrusan%20%26%20krambeck%2C%202006"));
        JsonNode second = taxonNode.at("/related_identifiers/1");
        assertThat(second.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(second.get("identifier").asText(), is("10.1016/j.tpb.2006.05.001"));

        JsonNode creatorNames = taxonNode.at("/creators");

        assertThat(creatorNames.get(0).get("name").asText(), is("Abrusan, G."));
        assertThat(creatorNames.get(1).get("name").asText(), is("Krambeck, H.-J."));
        assertThat(creatorNames.size(), is(2));
    }

    @Test
    public void streamTaxoDrosToLineJsonBook() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.book.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        assertThat(StringUtils.startsWith(".Z.Nijhoff", ".Z"), is(true));

        assertThat(taxonNode.get(NON_JOURNAL_TITLE).textValue(), is("Catalogue of the described Diptera from South Asia. 222 pp."));
        assertThat(taxonNode.get("partof_title").textValue(), is("Catalogue of the described Diptera from South Asia. 222 pp."));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("publication_type").textValue(), is("book"));
        assertThat(taxonNode.get("imprint_publisher").textValue(), is("Nijhoff"));
    }

    @Test
    public void streamTaxoDrosToLineJsonCollection() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.collection.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        assertThat(StringUtils.startsWith(".Z.Nijhoff", ".Z"), is(true));

        assertThat(taxonNode.get("referenceId").textValue(), is("collection, zmc"));
        assertThat(taxonNode.get(NON_JOURNAL_TITLE).textValue(), is("Zoological Museum University of Copenhagen Universitetsparken 15 DK-2100 Copenhagen O Denmark"));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("publication_type").textValue(), is("other"));
        assertThat(taxonNode.get("collection").textValue(), is("collection, zmc"));
    }

    @Test
    public void streamTaxoDrosToLineJsonWithDOILowerCase() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.doi.lower.txt");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        JsonNode first = taxonNode.at("/related_identifiers/0");
        assertThat(first.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(first.get("identifier").asText(), is("urn:lsid:taxodros.uzh.ch:id:abd%20el-halim%20et%20al.%2C%202005"));
        JsonNode second = taxonNode.at("/related_identifiers/1");
        assertThat(second.get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(second.get("identifier").asText(), is("10.7868/S0016675814060150"));

    }

    @Test
    public void streamTaxoDrosToLineJsonWithMultilineFilename() throws IOException {
        assertAssumptions("DROS5.TEXT.longfilename.txt");
    }

    @Test
    public void urlEncodeFilename() throws ContentStreamException {
        String filename = "one two.pdf";
        String path = JavaScriptAndPythonFriendlyURLEncodingUtil.urlEncode(filename);
        assertThat(path, is("one%20two.pdf"));
    }

    @Test
    public void streamTaxoDros3ToLineJson() throws IOException {
        String[] jsonObjects = getResource("DROS3.TEXT.example.txt");
        assertThat(jsonObjects.length, is(3));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L8"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("taxodros-dros3"));
        assertThat(taxonNode.get("referenceId").asText(), is("abd el-halim et al., 2005"));
        JsonNode localities = taxonNode.get("locations");
        assertThat(localities, is(notNullValue()));
        assertThat(localities.isArray(), is(true));
        assertThat(localities.size(), is(2));
        assertThat(localities.get(0).get("place").asText(), is("kena"));
        assertThat(localities.get(1).get("place").asText(), is("sinai"));
        JsonNode keywords = taxonNode.get("keywords");
        assertThat(keywords, is(notNullValue()));
        assertThat(keywords.isArray(), is(true));
        assertThat(keywords.size(), is(3));
        assertThat(keywords.get(0).asText(), is("histrioides"));
        assertThat(keywords.get(1).asText(), is("distr$"));
        assertThat(keywords.get(2).asText(), is("egypt"));
    }

    @Test
    public void streamSYSToLineJson() throws IOException {
        String[] jsonObjects = getResource("SYS.TEXT.example.txt");
        assertThat(jsonObjects.length, is(9));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText()
                , is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText()
                , is("taxodros-syst"));
        assertThat(taxonNode.get("taxonId").asText(), is("urn:lsid:taxodros.uzh.ch:taxon:abarista"));
        assertThat(taxonNode.get(".KF").asText(), is("abarista"));
        assertThat(taxonNode.get("genus").asText(), is("Cladochaeta"));
        assertThat(taxonNode.get("family").asText(), is("Drosophilidae"));
        assertThat(taxonNode.get("tribe").asText(), is("Cladochaetini"));
        assertThat(taxonNode.get("acceptedName").asText(), is("abarista"));
        assertThat(taxonNode.get("originalSpecificEpithet").asText(), is("abarista Grimaldi and Nguyen, 1999:254"));
        assertThat(taxonNode.get("originalGenus").asText(), is("Cladochaeta"));
        assertThat(taxonNode.get("accordingTo").asText(), is("grimaldi & nguyen, 1999"));
        assertThat(taxonNode.get("referenceId").asText(), is("grimaldi & nguyen, 1999"));
        assertThat(taxonNode.get(".ST").asText(), is(""));
        assertThat(taxonNode.get("remarks").asText(), is(""));
    }

    @Test
    public void parseBibliographicCitation() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.journal.txt");
        assertThat(jsonObjects.length, is(1));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText()
                , is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L9"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText()
                , is("taxodros-dros5"));
        assertThat(taxonNode.get("journal_title").asText(), is("Bull. Inst. Sci. Ind., Melb."));
        assertThat(taxonNode.get("journal_volume").asText(), is("29"));
        assertThat(taxonNode.get("journal_issue"), is(nullValue()));
        assertThat(taxonNode.get("journal_pages").asText(), is("1-"));

    }

    @Test
    public void parseBookChapterCitation() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.bookchapter.txt");
        assertThat(jsonObjects.length, is(1));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText()
                , is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L15"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText()
                , is("taxodros-dros5"));
        assertThat(taxonNode.get("upload_type").asText(), is("publication"));
        assertThat(taxonNode.get("publication_type").asText(), is("section"));
        assertThat(taxonNode.get(NON_JOURNAL_TITLE).asText(), is("La prevenzione delle contaminazioni entomatiche negli ortaggi per l'industria alimentare: sperimentazione di una macchina aspiratrice su colture di spinacio. In: Cravedi, P. (ed.), Atti del 60 Simposio \"La difesa antiparassitaria nelle industrie alimentari e la protezione degli alimenti\", pp. 57-65."));
        assertThat(taxonNode.get("partof_title").asText(), is("La prevenzione delle contaminazioni entomatiche negli ortaggi per l'industria alimentare: sperimentazione di una macchina aspiratrice su colture di spinacio. In: Cravedi, P. (ed.), Atti del 60 Simposio \"La difesa antiparassitaria nelle industrie alimentari e la protezione degli alimenti\""));
        assertThat(taxonNode.get("partof_pages").asText(), is("57-65"));
    }

    @Test
    public void parseYassin2009a() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.yassin2009a.txt");
        assertThat(jsonObjects.length, is(1));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        String expectedPrettyString = IOUtils.toString(getClass().getResourceAsStream("taxodros/DROS5.TEXT.yassin2009a.zenodo.json"), StandardCharsets.UTF_8);

        assertThat(expectedPrettyString, is(taxonNode.toPrettyString()));
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText()
                , is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L12"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText()
                , is("taxodros-dros5"));
        assertThat(taxonNode.get("upload_type").asText(), is("publication"));
        assertThat(taxonNode.get("publication_type").asText(), is("article"));
        assertThat(taxonNode.get(NON_JOURNAL_TITLE).asText(), is("Evolutionary Genetics of Zaprionus. II. Mitochondrial DNA and chromosomal variation of the invasive drosophilid Zaprionus indianus in Egypt."));
    }


    @Test
    public void parseBookChapterCitationSinglePage() throws IOException {
        String[] jsonObjects = getResource("DROS5.TEXT.bookchapter.single.page.txt");
        assertThat(jsonObjects.length, is(1));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText()
                , is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L15"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText()
                , is("taxodros-dros5"));
        assertThat(taxonNode.get("upload_type").asText(), is("publication"));
        assertDescription(taxonNode);

        assertThat(taxonNode.get("publication_type").asText(), is("section"));
        assertThat(taxonNode.get(NON_JOURNAL_TITLE).asText(), is("La prevenzione delle contaminazioni entomatiche negli ortaggi per l'industria alimentare: sperimentazione di una macchina aspiratrice su colture di spinacio. In: Cravedi, P. (ed.), Atti del 60 Simposio \"La difesa antiparassitaria nelle industrie alimentari e la protezione degli alimenti\", p. 57."));
        assertThat(taxonNode.get("partof_title").asText(), is("La prevenzione delle contaminazioni entomatiche negli ortaggi per l'industria alimentare: sperimentazione di una macchina aspiratrice su colture di spinacio. In: Cravedi, P. (ed.), Atti del 60 Simposio \"La difesa antiparassitaria nelle industrie alimentari e la protezione degli alimenti\""));
        assertThat(taxonNode.get("partof_pages").asText(), is("57"));
    }

    private void assertDescription(JsonNode taxonNode) {
        assertThat(taxonNode.get("description").asText(), is("Uploaded by Plazi for TaxoDros. We do not have abstracts."));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

    @Test
    public void parseJournalInfoWithNumber() {
        ObjectNode ref = new ObjectMapper().createObjectNode();
        TaxoDrosFileStreamHandler.enrichWithJournalInfo(ref, "32(1981):107");

        assertThat(ref.get("journal_pages").asText(), is("107"));
        assertThat(ref.get("journal_volume").asText(), is("32"));
        assertThat(ref.get("journal_issue").asText(), is("1981"));
    }

    @Test
    public void parseJournalInfo2() {
        ObjectNode ref = new ObjectMapper().createObjectNode();
        TaxoDrosFileStreamHandler.enrichWithJournalInfo(ref, "32:107");

        assertThat(ref.get("journal_pages").asText(), is("107"));
        assertThat(ref.get("journal_volume").asText(), is("32"));
    }

    @Test
    public void parseJournalInfo3() {
        ObjectNode ref = new ObjectMapper().createObjectNode();
        TaxoDrosFileStreamHandler.enrichWithJournalInfo(ref, "35:351-362.");

        assertThat(ref.get("journal_pages").asText(), is("351-362"));
        assertThat(ref.get("journal_volume").asText(), is("35"));
    }

    private void assertAssumptions(String testResource) throws IOException {
        String[] jsonObjects = getResource(testResource);
        assertThat(jsonObjects.length, is(3));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);


        assertDescription(taxonNode);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L10"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("taxodros-dros5"));
        assertThat(taxonNode.get("referenceId").asText(), is("abd el-halim et al., 2005"));
        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(2));
        assertThat(communities.get(0).get("identifier").asText(), is("taxodros"));
        assertThat(communities.get(1).get("identifier").asText(), is("biosyslit"));

        JsonNode creators = taxonNode.get("creators");
        assertThat(creators.isArray(), is(true));
        assertThat(creators.size(), is(3));
        assertThat(creators.get(0).get("name").asText(), is("Abd El-Halim, A.S."));
        assertThat(creators.get(1).get("name").asText(), is("Mostafa, A.A."));
        assertThat(creators.get(2).get("name").asText(), is("Allam, K.A.M.a."));
        assertThat(taxonNode.get(NON_JOURNAL_TITLE).asText(), is("Dipterous flies species and their densities in fourteen Egyptian governorates."));
        assertThat(taxonNode.get("journal_title").asText(), is("J. Egypt. Soc. Parasitol."));
        assertThat(taxonNode.get("journal_volume").asText(), is("35"));
        assertThat(taxonNode.get("journal_issue"), is(nullValue()));
        assertThat(taxonNode.get("journal_pages").asText(), is("351-362"));
        assertThat(taxonNode.get("publication_date").asText(), is("2005"));
        assertThat(taxonNode.get("access_right").asText(), is("restricted"));
        assertThat(taxonNode.get("taxodros:method").asText(), is("ocr"));
        assertThat(taxonNode.get("publication_type").textValue(), is("article"));
        assertThat(taxonNode.get("filename").asText(), is("Abd El-Halim et al., 2005M.pdf"));
        assertThat(taxonNode.get("references").get(0).asText(), is("Bächli, G. (1999). TaxoDros - The Database on Taxonomy of Drosophilidae hash://md5/abcd hash://sha256/efgh [Data set]. Zenodo. https://doi.org/10.123/456"));

        JsonNode keywords = taxonNode.at("/keywords");
        assertThat(keywords.get(0).asText(), is("Biodiversity"));
        JsonNode custom = taxonNode.at("/custom");
        assertThat(custom.toString(), is("{\"dwc:kingdom\":[\"Animalia\"],\"dwc:phylum\":[\"Arthropoda\"],\"dwc:class\":[\"Insecta\"],\"dwc:order\":[\"Diptera\"]}"));
    }

    private String[] getResource(String testResource) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("taxodros/" + testResource);
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
                byteArrayOutputStream,
                Arrays.asList("taxodros", "biosyslit"),
                new Properties() {{
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_DOI, "10.123/456");
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_VERSION_SHA256, "hash://sha256/efgh");
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_VERSION_MD5, "hash://md5/abcd");
                    setProperty(TaxoDrosFileStreamHandler.PROP_TAXODROS_DATA_YEAR, "1999");
                }}
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        return StringUtils.split(actual, "\n");
    }


}
