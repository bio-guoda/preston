package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
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

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class ZoteroFileExtractorTest {

    public static final String NON_JOURNAL_TITLE = "title";

    @Test
    public void streamZoteroArticleToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ZoteroArticle.json");
        assertArticleItem(jsonObjects);
    }

    @Test
    public void streamZoteroArticleListToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ZoteroArticleList.json");
        assertArticleItem(jsonObjects);
    }

    private void assertArticleItem(String[] jsonObjects) throws JsonProcessingException {
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);

        assertThat(taxonNode.get("description").asText(), is("Uploaded by Plazi for Bat Literature Project. We do not have abstracts."));

        JsonNode communities = taxonNode.get("communities");
        assertThat(communities.isArray(), is(true));
        assertThat(communities.size(), is(2));
        assertThat(communities.get(0).get("identifier").asText(), is("batlit"));
        assertThat(communities.get(1).get("identifier").asText(), is("biosyslit"));


        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/json+zotero"));
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
        assertThat(taxonNode.get("doi").textValue(), is("10.1093/gbe/evac018"));


        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(2));
        assertThat(identifiers.get(0).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://api.zotero.org/groups/5435545/items/P4LGETPS/file/view"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("hash://md5/45799372d92d29739588a8b8e5e35a8c"));

        JsonNode keywords = taxonNode.at("/keywords");
        assertThat(keywords.get(0).asText(), is("Biodiversity"));
        JsonNode custom = taxonNode.at("/custom");
        assertThat(custom.toString(), is("{\"dwc:kingdom\":[\"Animalia\"],\"dwc:phylum\":[\"Chordata\"],\"dwc:class\":[\"Mammalia\"],\"dwc:order\":[\"Chiroptera\"]}"));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
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
                } else if (StringUtils.equals("https://api.zotero.org/groups/5435545/items/P4LGETPS/file/view", key.getIRIString())) {
                    return IOUtils.toInputStream("this is a scientific article about bats", StandardCharsets.UTF_8);
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
        StatementsListener extractor = new ZoteroFileExtractor(
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
