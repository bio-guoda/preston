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
import org.hamcrest.core.Is;
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
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class ZoteroFileExtractorTest {

    @Test
    public void streamZoteroAttachmentToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ZoteroAttachment.json", "ZoteroArticle.json");
        assertArticleItem(jsonObjects);
    }

    @Test
    public void streamZoteroArticleToZenodoLineJsonWithTags() throws IOException {
        String[] jsonObjects = getResource("ZoteroAttachment.json", "ZoteroArticleWithTags.json");

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        JsonNode keywords = taxonNode.at("/keywords");
        List<String> keywordList = new ArrayList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(keywordList, hasItem("Molecular phylogeny"));

    }

    @Test
    public void streamZoteroBook() throws IOException {
        String[] jsonObjects = getResource("ZoteroBookAttachment.json", "ZoteroBook.json");

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        JsonNode keywords = taxonNode.at("/keywords");
        List<String> keywordList = new ArrayList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(keywordList, hasItem("Molecular phylogeny"));

    }

    @Test
    public void streamZoteroArticleListToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ZoteroAttachment.json", "ZoteroArticleList.json");
        assertThat(jsonObjects.length, Is.is(0));
    }

    private void assertArticleItem(String[] jsonObjects) throws JsonProcessingException {
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        assertThat(taxonNode.get("description").asText(), is("(Uploaded by Plazi for the Bat Literature Project) Exploring the natural origins of SARS-CoV-2 Spyros Lytras1, Joseph Hughes1, Xiaowei Jiang2, David L Robertson1  1MRC-University of Glasgow Centre for Virus Research (CVR), Glasgow, UK.  2Department of Biological Sciences, Xi’an Jiaotong-Liverpool University (XJTLU), Suzhou, China.  The lack of an identifiable intermediate host species for the proximal animal ancestor of SARS-CoV-2 and the distance (~1500 km) from Wuhan to Yunnan province, where the closest evolutionary related coronaviruses circ..."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(2));
        assertThat(communities.get(0).get("identifier").asText(), is("batlit"));
        assertThat(communities.get(1).get("identifier").asText(), is("biosyslit"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("https://linker.bio/hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/json"));
        assertThat(taxonNode.get("referenceId").asText(), is("https://api.zotero.org/groups/5435545/items/DP629R8S"));

        JsonNode creators = taxonNode.get("creators");
        assertThat(creators.isArray(), is(true));
        assertThat(creators.size(), is(10));
        assertThat(creators.get(0).get("name").asText(), is("Lytras, Spyros"));
        assertThat(creators.get(1).get("name").asText(), is("Hughes, Joseph"));
        assertThat(creators.get(9).get("name").asText(), is("Robertson, David L."));
        assertThat(taxonNode.get("journal_title").asText(), is("Genome Biology and Evolution"));
        assertThat(taxonNode.get("journal_volume").asText(), is("14"));
        assertThat(taxonNode.get("journal_issue").asText(), is("2"));
        assertThat(taxonNode.get("journal_pages").asText(), is("1-14"));
        assertThat(taxonNode.get("publication_date").asText(), is("2022"));
        assertThat(taxonNode.get("access_right").asText(), is("restricted"));
        assertThat(taxonNode.get("publication_type").textValue(), is("article"));
        assertThat(taxonNode.get("upload_type").textValue(), is("publication"));
        assertThat(taxonNode.get("doi"), is(nullValue()));
        assertThat(taxonNode.get("filename").textValue(), is("evac018.pdf"));


        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(9));
        // provided by Zoteros
        assertThat(identifiers.get(0).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("hash://md5/00335a95492b82cc0862e6bcc88497c4"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("urn:lsid:zotero.org:groups:5435545:items:DP629R8S"));

        // calculated on the fly
        assertThat(identifiers.get(2).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("https://linker.bio/hash://md5/a51c3c32b083b50d00f34bd72fcd3a19"));
        assertThat(identifiers.get(3).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("https://linker.bio/hash://sha256/4448f9919eb64bdd320eb9076430c84f792d8ebfe9c15ed7e020f439131eba5f"));
        assertThat(identifiers.get(3).has("resource_type"), is(false));

        // html landing pages
        assertThat(identifiers.get(4).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(4).get("identifier").asText(), is("zotero://select/groups/5435545/items/DP629R8S"));
        assertThat(identifiers.get(5).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(5).get("identifier").asText(), is("https://zotero.org/groups/5435545/items/DP629R8S"));
        assertThat(identifiers.get(6).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(6).get("identifier").asText(), is("https://linker.bio/hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(identifiers.get(7).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(7).get("identifier").asText(), is("10.1093/gbe/evac018"));
        assertThat(identifiers.get(8).get("relation").asText(), is("isCompiledBy"));
        assertThat(identifiers.get(8).get("identifier").asText(), is("10.5281/zenodo.1410543"));
        assertThat(identifiers.get(8).get("resource_type").asText(), is("software"));

        JsonNode keywords = taxonNode.at("/keywords");
        assertThat(keywords.get(0).asText(), is("Biodiversity"));

        JsonNode custom = taxonNode.at("/custom");
        assertThat(custom.toString(), is("{\"dwc:kingdom\":[\"Animalia\"],\"dwc:phylum\":[\"Chordata\"],\"dwc:class\":[\"Mammalia\"],\"dwc:order\":[\"Chiroptera\"]}"));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

    private String[] getResource(String testAttachment, final String testArticle) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource(testAttachment);
                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
                } else if (StringUtils.equals("hash://sha256/9e088b29db63c9c6f41cf6bc183cb61554f317656f8f34638a07398342da2b1a", key.getIRIString())) {
                    return IOUtils.toInputStream("this is a scientific article about bats associated with https://api.zotero.org/groups/5435545/items/P4LGETPS/file/view", StandardCharsets.UTF_8);
                } else if (StringUtils.equals("hash://sha256/9f088b29db63c9c6f41cf6bc183cb61554f317656f8f34638a07398342da2b1a", key.getIRIString())) {
                    return getClass().getResourceAsStream(testArticle);
                }
                throw new RuntimeException("unresolved [" + key + "]");
            }
        };

        Quad statement = toStatement(
                toIRI("blip"),
                HAS_VERSION,
                toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1")
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        Persisting processorState = new Persisting();
        URL resource = getClass().getResource("/bio/guoda/preston/cmd/zoterodatadir/2a/5d/2a5de79372318317a382ea9a2cef069780b852b01210ef59e06b640a3539cb5a");
        File dataDir = new File(resource.getFile()).getParentFile().getParentFile().getParentFile();
        processorState.setLocalDataDir(dataDir.getAbsolutePath());

        StatementsListener extractor = new ZoteroFileExtractor(
                processorState,
                blobStore,
                byteArrayOutputStream,
                Arrays.asList("batlit", "biosyslit")
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        return StringUtils.split(actual, "\n");
    }


}
