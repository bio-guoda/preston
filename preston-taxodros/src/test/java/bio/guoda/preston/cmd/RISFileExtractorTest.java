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
import static org.hamcrest.number.OrderingComparison.greaterThan;

public class RISFileExtractorTest {

    @Test
    public void streamZoteroAttachmentToZenodoLineJson() throws IOException {
        String[] jsonObjects = getResource("ris/bhlpart-multiple.ris");
        assertArticleItem(jsonObjects);
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
        assertThat(taxonNode.get("filename").textValue(), is("https://www.biodiversitylibrary.org/partpdf/332157"));


        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(4));
        // provided by Zoteros
        assertThat(identifiers.get(0).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L22"));
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("10.3897/mycokeys.85.73405"));

        // calculated on the fly
        assertThat(identifiers.get(2).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("https://www.biodiversitylibrary.org/part/332157"));
        assertThat(identifiers.get(3).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("https://www.biodiversitylibrary.org/partpdf/332157"));
        assertThat(identifiers.get(3).has("resource_type"), is(false));

        JsonNode keywords = taxonNode.at("/keywords");
        assertThat(keywords.get(0).asText(), is("Entomophthorales"));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

    private String[] getResource(String records) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource(records);
                IRI iri = toIRI(resource.toExternalForm());

                if (StringUtils.equals("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1", key.getIRIString())) {
                    try {
                        return new FileInputStream(new File(URI.create(iri.getIRIString())));
                    } catch (FileNotFoundException e) {
                        throw new RuntimeException(e);
                    }
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

        StatementsListener extractor = new RISFileExtractor(
                processorState,
                blobStore,
                byteArrayOutputStream,
                Arrays.asList("biosyslit")
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        return StringUtils.split(actual, "\n");
    }


}
