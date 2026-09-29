package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
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
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class EppoDatabaseExtractorTest {

    @Test
    public void singlePestRecord() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("CUNNSP_pests.json");
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
                toIRI("https://api.eppo.int/gd/v2/taxons/taxon/CUNNSP/pests"),
                HAS_VERSION,
                toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1")
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        EppoDatabaseExtractor recordExtractor = new EppoDatabaseExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        recordExtractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasRevisionOf").asText(), is("https://api.eppo.int/gd/v2/taxons/taxon/CUNNSP/pests"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/json"));
        assertThat(taxonNode.get("eppocode").asText(), is("EUWAWH"));
        assertThat(taxonNode.get("prefname").asText(), is("Euwallacea fornicatus sensu stricto"));
        assertThat(taxonNode.get("class_id").asText(), is("9"));
        assertThat(taxonNode.get("class_label").asText(), is("Host"));
        assertThat(taxonNode.get("bibref").asText(), is("* Smith SM, Gomez DF, Beaver RA, Hulcr J, Cognato AI (2019) Reassessment of the species in the Euwallacea fornicatus (Coleoptera: Curculionidae: Scolytinae) complex after the rediscovery of the ‘lost’ type specimen. Insects 10, 261. https://doi.org/10.3390/insects10090261"));
    }


}