package bio.guoda.preston.process;

import bio.guoda.preston.cmd.DwcRecordExtractor;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.isEmptyString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

public class DwcRecordExtractorTest {

    @Test
    public void streamDwcRecordsToJSON() throws IOException {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/plazidwca.zip");
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
        DwcRecordExtractor dwcRecordExtractor = new DwcRecordExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        dwcRecordExtractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(15));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:zip:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/taxa.txt!/L2"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("http://rs.tdwg.org/dwc/terms/Taxon"));
        assertThat(taxonNode.get("http://rs.tdwg.org/dwc/text/id").asText(), is("D51D87C0FFC4C76F4B9C5298FC31DFDF.taxon"));
        assertThat(taxonNode.get("http://rs.tdwg.org/dwc/terms/scientificName").asText(), is("Calyptraeotheres Campos 1990"));

        for (String jsonString : jsonObjects) {
            JsonNode documentNode = new ObjectMapper().readTree(jsonString);
            if (StringUtils.equals(
                    documentNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(),
                    "http://rs.tdwg.org/dwc/terms/Taxon")
            ) {
                assertThat(documentNode.get("http://rs.tdwg.org/dwc/text/id").asText(), not(isEmptyString()));
                assertNull(documentNode.get("http://rs.tdwg.org/dwc/text/coreid"));
            } else {
                assertThat(documentNode.get("http://rs.tdwg.org/dwc/text/coreid").asText(), not(isEmptyString()));
                assertNull(documentNode.get("http://rs.tdwg.org/dwc/text/id"));
            }

        }

    }

    @Test
    public void errorOnStreamingInvalidDwcRecordsToJSON() throws IOException {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/plazidwca-broken-meta.xml.zip");
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


        Quad statement = toStatement(toIRI("blip"), HAS_VERSION, toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DwcRecordExtractor dwcRecordExtractor = new DwcRecordExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        try {
            dwcRecordExtractor.on(statement);
        } catch (RuntimeException ex) {
            throw ex;
        }

        String actual = IOUtils.toString(byteArrayOutputStream.toByteArray(), StandardCharsets.UTF_8.name());

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(0));
    }

    @Ignore
    @Test
    public void streamEncyclopediaOfLife() throws IOException {

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/dunnetal2015.zip");
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
        DwcRecordExtractor dwcRecordExtractor = new DwcRecordExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        dwcRecordExtractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(15));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:zip:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/taxa.txt!/L2"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("http://rs.tdwg.org/dwc/terms/Taxon"));
        assertThat(taxonNode.get("http://rs.tdwg.org/dwc/text/id").asText(), is("D51D87C0FFC4C76F4B9C5298FC31DFDF.taxon"));
        assertThat(taxonNode.get("http://rs.tdwg.org/dwc/terms/scientificName").asText(), is("Calyptraeotheres Campos 1990"));

        for (String jsonString : jsonObjects) {
            JsonNode documentNode = new ObjectMapper().readTree(jsonString);
            if (StringUtils.equals(
                    documentNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(),
                    "http://rs.tdwg.org/dwc/terms/Taxon")
            ) {
                assertThat(documentNode.get("http://rs.tdwg.org/dwc/text/id").asText(), not(isEmptyString()));
                assertNull(documentNode.get("http://rs.tdwg.org/dwc/text/coreid"));
            } else {
                assertThat(documentNode.get("http://rs.tdwg.org/dwc/text/coreid").asText(), not(isEmptyString()));
                assertNull(documentNode.get("http://rs.tdwg.org/dwc/text/id"));
            }

        }

    }


}
