package bio.guoda.preston.cmd;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Hasher;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections4.list.TreeList;
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
import static org.hamcrest.number.OrderingComparison.greaterThan;
import static org.junit.Assert.assertNotNull;

public class ZoteroFileExtractorZenodoTest {

    public static final String CONTENT_OF_MD5_0033 = "a pdf associated with https://api.zotero.org/groups/5435545/items/P4LGETPS/file/view";

    @Test
    public void streamZoteroAttachmentToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ZoteroAttachment.json", "ZoteroArticle.json", Arrays.asList("batlit", "biosyslit"), false);
        assertArticleItem(jsonObjects);
    }


    @Test
    public void skipZoteroRecordWithSuspiciousDate() throws IOException {
        String[] jsonObjects = getResource("ZoteroArticleSuspiciousDateAttachment.json", "ZoteroArticleSuspiciousDate.json", Arrays.asList("batlit", "biosyslit"), false);
        assertThat(jsonObjects.length, is(0));
    }

    @Test
    public void zoteroRecordIPBESWithDoiInTitle() throws IOException {
        String expectedTitle = "Invasive plant species and their disaster-effects in dry tropical forests and rangelands of Kenya and Tanzania https://doi.org/10.4102/jamba.v3i2.39";
        boolean includeProvidedDoiInTitle = true;
        assertIPBESMetadata(includeProvidedDoiInTitle, expectedTitle);
    }

    @Test
    public void zoteroRecordIPBESWithDoiAsVersionOf() throws IOException {
        boolean includeProvidedDoiInTitle = true;
        String[] jsonObjects = getResource(
                "ZoteroArticleIPBESAttachment.json",
                "ZoteroArticleIPBES.json",
                Arrays.asList("ipbes-ias", "biosyslit"),
                includeProvidedDoiInTitle
        );
        assertThat(jsonObjects.length, is(1));
        JsonNode metadata = unwrapMetadata(jsonObjects[0]);
        JsonNode relatedIdentifiers = metadata.at("/related_identifiers");
        JsonNode variantFormOf = null;
        for (JsonNode relatedIdentifier : relatedIdentifiers) {
            if (relatedIdentifier.has("relation")
                    && StringUtils.equals(relatedIdentifier.get("relation").asText(), "isVariantFormOf")) {
                variantFormOf = relatedIdentifier;
            }
        }

        assertNotNull(variantFormOf);

        assertThat(variantFormOf.get("identifier").asText(), is("10.4102/jamba.v3i2.39"));
        assertThat(variantFormOf.get("resource_type").asText(), is("publication"));

        JsonNode keywords = metadata.at("/keywords");
        List<String> keywordList = new TreeList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(StringUtils.join(keywordList, ";"), is("Chapter 4;biodiversity;environment assessment;IPBES;Alien Invasive Species Assessment AIS;invasive species"));
    }

    @Test
    public void zoteroRecordIPBESWithoutDoiInTitle() throws IOException {
        String expectedTitle = "Invasive plant species and their disaster-effects in dry tropical forests and rangelands of Kenya and Tanzania";
        boolean includeProvidedDoiInTitle = false;
        assertIPBESMetadata(includeProvidedDoiInTitle, expectedTitle);
    }

    private void assertIPBESMetadata(boolean includeProvidedDoiInTitle, String expectedTitle) throws IOException {
        String[] jsonObjects = getResource(
                "ZoteroArticleIPBESAttachment.json",
                "ZoteroArticleIPBES.json",
                Arrays.asList("ipbes-ias", "biosyslit"),
                includeProvidedDoiInTitle
        );
        assertThat(jsonObjects.length, is(1));
        JsonNode metadata = unwrapMetadata(jsonObjects[0]);
        JsonNode relatedIdentifiers = metadata.at("/related_identifiers");
        JsonNode citedBy = null;
        for (JsonNode relatedIdentifier : relatedIdentifiers) {
            if (relatedIdentifier.has("relation")
                    && StringUtils.equals(relatedIdentifier.get("relation").asText(), "isCitedBy")) {
                citedBy = relatedIdentifier;
            }
        }

        assertNotNull(citedBy);

        assertThat(citedBy.get("resource_type").asText(), is("publication-report"));
        assertThat(citedBy.get("identifier").asText(), is("10.5281/zenodo.7430682"));

        assertThat(metadata.at("/title").asText(),
                is(expectedTitle));

        JsonNode keywords = metadata.at("/keywords");
        List<String> keywordList = new TreeList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(StringUtils.join(keywordList, ";"), is("Chapter 4;biodiversity;environment assessment;IPBES;Alien Invasive Species Assessment AIS;invasive species"));
    }

    @Test
    public void streamZoteroArticleToZenodoLineJsonWithTags() throws IOException {
        String[] jsonObjects = getResource("ZoteroAttachment.json", "ZoteroArticleWithTags.json", Arrays.asList("batlit", "biosyslit"), false);

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        JsonNode keywords = taxonNode.at("/keywords");
        List<String> keywordList = new ArrayList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(keywordList, hasItem("Molecular phylogeny"));

    }

    @Test
    public void streamZoteroBook() throws IOException {
        String[] jsonObjects = getResource("ZoteroBookAttachment.json", "ZoteroBook.json", Arrays.asList("batlit", "biosyslit"), false);

        assertThat(jsonObjects.length, is(greaterThan(0)));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        JsonNode keywords = taxonNode.at("/keywords");
        List<String> keywordList = new ArrayList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(keywordList, hasItem("Biodiversity"));

        assertThat(taxonNode.at("/publication_type").asText(), is("book"));

    }

    @Test
    public void zoteroNewsArticle() throws IOException {
        String[] jsonObjects = getResource("ZoteroNewsArticleAttachment.json", "ZoteroNewsArticle.json", Arrays.asList("batlit", "biosyslit"), false);

        assertThat(jsonObjects.length, is(greaterThan(0)));
        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);
        JsonNode keywords = taxonNode.at("/keywords");
        List<String> keywordList = new ArrayList<>();
        keywords.forEach(k -> keywordList.add(k.asText()));
        assertThat(keywordList, hasItem("Biodiversity"));

        assertThat(taxonNode.at("/publication_type").asText(), is("other"));

    }

    @Test
    public void streamZoteroArticleListToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ZoteroAttachment.json", "ZoteroArticleList.json", Arrays.asList("batlit", "biosyslit"), false);
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
        assertThat(identifiers.size(), is(10));
        // provided by Zoteros
        assertThat(identifiers.get(0).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("hash://md5/00335a95492b82cc0862e6bcc88497c4"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("urn:lsid:zotero.org:groups:5435545:items:DP629R8S"));

        // calculated on the fly
        assertThat(identifiers.get(2).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(2).get("identifier").asText(), is(Hasher.calcHashIRI(CONTENT_OF_MD5_0033, HashType.md5).getIRIString()));
        assertThat(identifiers.get(3).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(3).get("identifier").asText(), is(Hasher.calcHashIRI(CONTENT_OF_MD5_0033, HashType.sha256).getIRIString()));
        assertThat(identifiers.get(3).has("resource_type"), is(false));

        // html landing pages
        assertThat(identifiers.get(4).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(4).get("identifier").asText(), is("zotero://select/groups/5435545/items/DP629R8S"));
        assertThat(identifiers.get(5).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(5).get("identifier").asText(), is("https://zotero.org/groups/5435545/items/DP629R8S"));
        assertThat(identifiers.get(6).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(6).get("identifier").asText(), is("https://linker.bio/hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(identifiers.get(7).get("relation").asText(), is("isPartOf"));
        assertThat(identifiers.get(7).get("identifier").asText(), is("some:anchor"));
        assertThat(identifiers.get(8).get("relation").asText(), is("isVariantFormOf"));
        assertThat(identifiers.get(8).get("identifier").asText(), is("10.1093/gbe/evac018"));
        assertThat(identifiers.get(9).get("relation").asText(), is("isCompiledBy"));
        assertThat(identifiers.get(9).get("identifier").asText(), is("10.5281/zenodo.1410543"));
        assertThat(identifiers.get(9).get("resource_type").asText(), is("software"));

        JsonNode keywords = taxonNode.at("/keywords");
        assertThat(keywords.get(0).asText(), is("Biodiversity"));

        JsonNode custom = taxonNode.at("/custom");
        assertThat(custom.toString(), is("{\"dwc:kingdom\":[\"Animalia\"],\"dwc:phylum\":[\"Chordata\"],\"dwc:class\":[\"Mammalia\"],\"dwc:order\":[\"Chiroptera\"]}"));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

    private String[] getResource(String testAttachment, final String testArticle, List<String> communities, boolean includeProvidedDoiInTitle) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("zotero/" + testAttachment);
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
                    return getClass().getResourceAsStream("zotero/" + testArticle);
                } else if (StringUtils.equals("hash://md5/1ffc3d79f67156dd7b4aa5e963a95345", key.getIRIString())) {
                    return IOUtils.toInputStream("a pdf associated with https://api.zotero.org/groups/5435545/items/8PZ5M3FX/file/view", StandardCharsets.UTF_8);
                } else if (StringUtils.equals("hash://md5/48226913783fbc2af44fb5cd2ac460d5", key.getIRIString())) {
                    return IOUtils.toInputStream("a pdf associated with https://api.zotero.org/groups/5435545/items/AJAULP6T/file/view", StandardCharsets.UTF_8);
                } else if (StringUtils.equals("hash://md5/00335a95492b82cc0862e6bcc88497c4", key.getIRIString())) {
                    return IOUtils.toInputStream(CONTENT_OF_MD5_0033, StandardCharsets.UTF_8);
                } else if (StringUtils.equals("hash://md5/d0c82eeff5a972dadadc58afea509601", key.getIRIString())) {
                    return IOUtils.toInputStream("content of some pdf", StandardCharsets.UTF_8);
                } else if (StringUtils.equals("hash://md5/26225b23191d5cf93c3fa30aae54f0ac", key.getIRIString())) {
                    return IOUtils.toInputStream("content of some pdf", StandardCharsets.UTF_8);
                } else if (StringUtils.equals("https://api.zotero.org/groups/5435545/items/C9PK97YL", key.getIRIString())) {
                    return getClass().getResourceAsStream("zotero/" + testArticle);
                } else if (StringUtils.equals("https://api.zotero.org/groups/5435545/items/UPBFKSQJ", key.getIRIString())) {
                    return getClass().getResourceAsStream("zotero/" + testArticle);
                } else if (StringUtils.equals("https://api.zotero.org/groups/5435545/items/TMSDEKSQ", key.getIRIString())) {
                    return getClass().getResourceAsStream("zotero/" + testArticle);
                } else if (StringUtils.equals("https://api.zotero.org/groups/5435545/items/DP629R8S", key.getIRIString())) {
                    return getClass().getResourceAsStream("zotero/" + testArticle);
                } else if (StringUtils.equals("https://api.zotero.org/groups/2352922/items/D2HF5WXT", key.getIRIString())) {
                    return getClass().getResourceAsStream("zotero/" + testArticle);
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
        processorState.setDataDir(dataDir.getAbsolutePath());

        StatementsListener extractor = new ZoteroFileExtractorZenodo(
                processorState,
                blobStore,
                byteArrayOutputStream,
                communities,
                RefNodeFactory.toIRI("some:anchor"),
                includeProvidedDoiInTitle
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        return StringUtils.split(actual, "\n");
    }


}
