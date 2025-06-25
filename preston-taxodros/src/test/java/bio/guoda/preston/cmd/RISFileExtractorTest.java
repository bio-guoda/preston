package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RISFileExtractorTest {

    @Test
    public void streamBHLPartsToZenodoLineJson() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-multiple.ris", "332157");
        assertArticleItem(jsonObjects);
    }

    @Test
    public void streamBHLSciELOPartsToZenodoLineJson() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-scielo.ris", "108952");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(1));
        assertThat(communities.get(0).get("identifier").asText(), is("biosyslit"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L12"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/x-research-info-systems"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://www.biodiversitylibrary.org/part/108952"));

        JsonNode creators = taxonNode.get("creators");
        assertThat(creators.isArray(), is(true));
        assertThat(creators.size(), is(1));
        assertThat(creators.get(0).get("name").asText(), is("Buys, Sandor Christiano."));
        assertThat(taxonNode.get("journal_title").asText(), is("Biota Neotropica"));
        assertThat(taxonNode.get("publication_date").asText(), is("2011-12-01"));
        assertThat(taxonNode.get("journal_issue"), is(nullValue()));
        assertThat(taxonNode.get("access_right"), is(nullValue()));
        assertThat(taxonNode.get("publication_type").textValue(), is("article"));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("filename").textValue(), is("bhlpart108952.pdf"));


        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(6));

        assertThat(identifiers.get(0).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L12"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(1).get("identifier").asText(), is(not("https://www.biodiversitylibrary.org/partpdf/108952")));
        assertThat(identifiers.get(1).has("resource_type"), is(false));

        assertThat(identifiers.get(1).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("https://www.biodiversitylibrary.org/part/108952"));

        assertThat(identifiers.get(2).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("urn:lsid:biodiversitylibrary.org:part:108952"));

        assertThat(identifiers.get(3).get("relation").asText(), is("isPartOf"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("hash://sha256/37171f648818b1286f7df81bca57c9b8c43d2e22d64c8520f7d2464e282cd6e0"));

        // calculated on the fly
        assertThat(identifiers.get(4).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(4).get("identifier").asText(), is("hash://md5/f3452e34cc97208fdac0d1375c94c7a2"));

        assertThat(identifiers.get(5).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(5).get("identifier").asText(), is("hash://sha256/da8e8a1b2579542779408c410edb110f9a44f4206db2df66ec46391bcba78015"));


        List<String> keywordList = getKeywordList(taxonNode);

        assertThat(keywordList, hasItem("Biodiversity"));
        assertThat(keywordList, hasItem("Aculeate"));
    }

    @Test
    public void streamBHLPartToZenodoLineJsonMissingAttachment() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-multiple.ris", "332157");
        assertThat(jsonObjects.length, is(5));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[1]);

        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(5));
        // provided by Zoteros
        assertThat(identifiers.get(0).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L23-L44"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("10.3897/subtbiol.43.85804"));

        assertThat(identifiers.get(2).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("https://www.biodiversitylibrary.org/part/337600"));

        assertThat(identifiers.get(3).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("urn:lsid:biodiversitylibrary.org:part:337600"));

        assertThat(identifiers.get(4).get("relation").asText(), is("isPartOf"));
        assertThat(identifiers.get(4).get("identifier").asText(), is("hash://sha256/37171f648818b1286f7df81bca57c9b8c43d2e22d64c8520f7d2464e282cd6e0"));

        List<String> keywordList = getKeywordList(taxonNode);

        assertThat(keywordList, hasItem("Biodiversity"));
        assertThat(keywordList, hasItem("cave"));
    }

    @Test
    public void streamBHLWithAuthorTrailingCommas() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-author-trailing-comma.ris", "44156");
        assertThat(jsonObjects.length, is(1));

        JsonNode metadata = unwrapMetadata(jsonObjects[0]);

        JsonNode creators = metadata.at("/creators/0");

        assertThat(creators.get("name").asText(), is("Rathbun, Mary Jane"));

    }

    @Test
    public void streamBHLWithJournalIssue() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-journal-issue.ris", "149328");
        assertThat(jsonObjects.length, is(1));

        JsonNode metadata = unwrapMetadata(jsonObjects[0]);

        assertThat(metadata.get("journal_pages").asText(), is("123-132"));
        assertThat(metadata.get("journal_title").asText(), is("Opinions and declarations rendered by the International Commission on Zoological Nomenclature"));
        assertThat(metadata.get("journal_volume").asText(), is("2"));
        assertThat(metadata.get("journal_issue").asText(), is("14"));
    }

    @Test
    public void streamBHLWithBiodiversityKeyword() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-journal-issue.ris", "149328");
        assertThat(jsonObjects.length, is(1));

        List<String> keywordsFound = getKeywords(jsonObjects[0]);

        assertThat(keywordsFound, hasItem("Biodiversity"));
        assertThat(keywordsFound, hasItem("BHL-Corpus"));

    }

    @Test
    public void streamBHLWithBiodiversityKeywordAndCustomKeywords() throws IOException {
        String[] jsonObjects = extractMetadata("ris/bhlpart-scielo.ris", "108952");
        assertThat(jsonObjects.length, is(1));

        List<String> keywordsFound = getKeywords(jsonObjects[0]);

        assertThat(keywordsFound, hasItem("Biodiversity"));
        assertThat(keywordsFound, hasItem("BHL-Corpus"));
        assertThat(keywordsFound, hasItem("Geographic distribution"));

    }

    private List<String> getKeywords(String jsonObject) throws JsonProcessingException {
        JsonNode metadata = unwrapMetadata(jsonObject);

        JsonNode keywords = metadata.get("keywords");

        List<String> keywordsFound = new ArrayList<>();
        for (JsonNode keyword : keywords) {
            assertThat(keyword.isTextual(), is(true));
            keywordsFound.add(keyword.asText());
        }
        return keywordsFound;
    }

    private void assertArticleItem(String[] jsonObjects) throws JsonProcessingException {
        assertThat(jsonObjects.length, is(5));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi from the Biodiversity Heritage Library) No abstract provided."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(1));
        assertThat(communities.get(0).get("identifier").asText(), is("biosyslit"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L22"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/x-research-info-systems"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://www.biodiversitylibrary.org/part/332157"));

        JsonNode creators = taxonNode.get("creators");
        assertThat(creators.isArray(), is(true));
        assertThat(creators.size(), is(7));
        assertThat(creators.get(0).get("name").asText(), is("Cai, Yue"));
        assertThat(creators.get(1).get("name").asText(), is("Nie, Yong"));
        assertThat(creators.get(6).get("name").asText(), is("Huang, Bo"));
        assertThat(taxonNode.get("journal_title").asText(), is("MycoKeys"));
        assertThat(taxonNode.get("journal_volume").asText(), is("85"));
        assertThat(taxonNode.get("journal_pages").asText(), is("161-172"));
        assertThat(taxonNode.get("publication_date").asText(), is("2021"));
        assertThat(taxonNode.get("journal_issue"), is(nullValue()));
        assertThat(taxonNode.get("access_right"), is(nullValue()));
        assertThat(taxonNode.get("publication_type").textValue(), is("article"));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("doi").textValue(), is("10.3897/mycokeys.85.73405"));
        assertThat(taxonNode.get("filename").textValue(), is("bhlpart332157.pdf"));


        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(7));

        assertThat(identifiers.get(0).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L22"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("10.3897/mycokeys.85.73405"));

        assertThat(identifiers.get(2).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("https://www.biodiversitylibrary.org/part/332157"));

        assertThat(identifiers.get(3).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("urn:lsid:biodiversitylibrary.org:part:332157"));

        assertThat(identifiers.get(4).get("relation").asText(), is("isPartOf"));
        assertThat(identifiers.get(4).get("identifier").asText(), is("hash://sha256/37171f648818b1286f7df81bca57c9b8c43d2e22d64c8520f7d2464e282cd6e0"));

        // calculated on the fly
        assertThat(identifiers.get(5).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(5).get("identifier").asText(), is("hash://md5/f3452e34cc97208fdac0d1375c94c7a2"));

        assertThat(identifiers.get(6).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(6).get("identifier").asText(), is("hash://sha256/da8e8a1b2579542779408c410edb110f9a44f4206db2df66ec46391bcba78015"));


        List<String> keywordList = getKeywordList(taxonNode);

        assertThat(keywordList, hasItem("Biodiversity"));
        assertThat(keywordList, hasItem("Entomophthorales"));
    }

    public static List<String> getKeywordList(JsonNode taxonNode) {
        JsonNode keywords = taxonNode.at("/keywords");

        List<String> keywordList = new ArrayList<>();

        for (JsonNode keyword : keywords) {
            keywordList.add(keyword.asText());
        }
        return keywordList;
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

    private String[] extractMetadata(String records, final String bhlPartId) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                URL resource = getClass().getResource(records);
                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else if (StringUtils.equals("https://www.biodiversitylibrary.org/partpdf/" + bhlPartId, key.getIRIString())) {
                    return IOUtils.toInputStream("hello! I am supposed to be a pdf...", StandardCharsets.UTF_8);
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
        Persisting processorState = new Persisting();
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/bhldatadir/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        File dataDir = new File(resource.getFile()).getParentFile().getParentFile().getParentFile();
        processorState.setDataDir(dataDir.getAbsolutePath());

        StatementsListener extractor = new RISFileExtractor(
                processorState,
                blobStore,
                byteArrayOutputStream,
                Arrays.asList("biosyslit"),
                true
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        return StringUtils.split(actual, "\n");
    }


}
