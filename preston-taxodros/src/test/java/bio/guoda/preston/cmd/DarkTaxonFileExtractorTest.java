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

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class DarkTaxonFileExtractorTest {

    @Test
    public void readmeToLineJSON() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("darktaxon/README");
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
        DarkTaxonFileExtractor extractor = new DarkTaxonFileExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(70));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);


        assertDescription(taxonNode);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L9"));
        assertThat(taxonNode.get("darktaxon:plateId").asText(), is("BMT121"));
        assertThat(taxonNode.get("darktaxon:specimenId").asText(), is("BMT0009397"));
        assertThat(taxonNode.get("darktaxon:imageFilePath").asText(), is("BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff"));
        assertThat(taxonNode.get("darktaxon:imageStackNumber").asText(), is("01"));
        assertThat(taxonNode.get("darktaxon:imageAcquisitionMethod").asText(), is("RAW"));
        assertThat(taxonNode.get("darktaxon:imageNumber").asText(), is("01"));
        assertThat(taxonNode.get("darktaxon:mimeType").asText(), is("image/tiff"));


        taxonNode = unwrapMetadata(jsonObjects[jsonObjects.length - 1]);


        assertDescription(taxonNode);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L78"));
        assertThat(taxonNode.get("darktaxon:plateId").asText(), is("BMT121"));
        assertThat(taxonNode.get("darktaxon:specimenId").asText(), is("BMT0009392"));
        assertThat(taxonNode.get("darktaxon:imageFilePath").asText(), is("BMT121/BMT0009392/BMT121_BMT0009392_stacked_04.tiff"));
        assertThat(taxonNode.get("darktaxon:imageStackNumber").asText(), is("04"));
        assertThat(taxonNode.get("darktaxon:imageNumber"), is(nullValue()));
        assertThat(taxonNode.get("darktaxon:imageAcquisitionMethod").asText(), is("stacked"));
        assertThat(taxonNode.get("darktaxon:mimeType").asText(), is("image/tiff"));

    }


    private void assertDescription(JsonNode taxonNode) {
        assertThat(taxonNode.get("description").asText(), is("Uploaded by Plazi for the Museum für Naturkunde Berlin."));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

}
