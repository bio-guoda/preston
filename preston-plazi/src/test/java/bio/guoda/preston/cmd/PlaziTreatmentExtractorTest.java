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

public class PlaziTreatmentExtractorTest {

    @Test
    public void streamSingleTreatmentFromZip() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/cmd/plazi-treatments.zip");
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

        JsonNode taxonNode = retrieveFirstJson(blobStore);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("zip:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/03D587F2FFC94C03F8F13AECFBD8F765.xml"));
        assertTreatmentValues(taxonNode);

    }

    @Test
    public void skipNonTreatmentInZip() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/cmd/skip-non-treatments.zip");
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
        PlaziTreatmentExtractor treatmentExractor = new PlaziTreatmentExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        treatmentExractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(1));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("zip:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/03BD87A2C660A212FF52F3F7F35547D7.xml"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/plazi+xml"));
        assertThat(taxonNode.get("docId").asText(), is("03BD87A2C660A212FF52F3F7F35547D7"));
        assertThat(taxonNode.get("interpretedGenus").asText(), is("Hipposideros"));
        assertThat(taxonNode.get("interpretedSpecies").asText(), is("tephrus"));

    }

    @Test
    public void onlyTwoTreatmentInZip() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/cmd/only-two-treatments.zip");
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
        PlaziTreatmentExtractor treatmentExractor = new PlaziTreatmentExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        treatmentExractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(),
                is("zip:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/038F4B5AFFA1FFD6A892FDF8B80ECA7D.xml"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/plazi+xml"));
        assertThat(taxonNode.get("docId").asText(), is("038F4B5AFFA1FFD6A892FDF8B80ECA7D"));
        assertThat(taxonNode.get("interpretedGenus").asText(), is("Fukomys"));
        assertThat(taxonNode.get("interpretedSpecies").asText(), is("damarensis"));

        assertThat(jsonObjects.length, is(2));


    }


    @Test
    public void streamSingleTreatmentFromXML() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/cmd/03D587F2FFC94C03F8F13AECFBD8F765.xml");
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

        JsonNode taxonNode = retrieveFirstJson(blobStore);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(),
                is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertTreatmentValues(taxonNode);

    }

    @Test
    public void streamSingleMSWTreatmentFromXML() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/cmd/0021FE787113F56516F6F69DFBCC22DC.xml");
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

        JsonNode taxonNode = retrieveFirstJson(blobStore);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(),
                is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/plazi+xml"));
        assertThat(taxonNode.get("docId").asText(), is("0021FE787113F56516F6F69DFBCC22DC"));
        assertThat(taxonNode.get("verbatimText").asText(), is("Subfamily Zenkerellinae Matschie, 1898 . Sitzb. Ges. Naturi. Fr. Berlin, 4:26 . SYNONYMS: Idiurinae Miller and Gidley, 1918 ."));
        assertThat(taxonNode.get("interpretedGenus"), is(nullValue()));
        assertThat(taxonNode.get("interpretedSpecies"), is(nullValue()));

    }

    @Test
    public void streamSingleMSWTreatmentFromXMLC30587A9A562FF93FF2F350F875ED55FGitHub() throws IOException {
        JsonNode taxonNode = assertBarbastella("/bio/guoda/preston/cmd/C30587A9A562FF93FF2F350F875ED55F.github.xml", "");
        assertThat(taxonNode.get("taxonomicNameId"), is(nullValue()));

    }

    @Test
    public void streamSingleMSWTreatmentFromXMLC30587A9A562FF93FF2F350F875ED55FDump() throws IOException {
        JsonNode taxonNode = assertBarbastella("/bio/guoda/preston/cmd/C30587A9A562FF93FF2F350F875ED55F.dump.xml", "");
        assertThat(taxonNode.get("taxonomicNameId"), is(nullValue()));
    }

    @Test
    public void streamSingleMSWTreatmentFromXMLC30587A9A562FF93FF2F350F875ED55FWeb() throws IOException {
        JsonNode taxonNode = assertBarbastella("/bio/guoda/preston/cmd/C30587A9A562FF93FF2F350F875ED55F.web.xml", "");
        assertThat(taxonNode.get("taxonomicNameId").asText(), is("8CAC4D3CA562FF93FF2F350F80EFD68D"));
    }

    @Test
    public void streamSingleMSWTreatmentFromXMLC30587A9A562FF93FF2F350F875ED55FHistorizedDump() throws IOException {
        JsonNode taxonNode = assertBarbastella("/bio/guoda/preston/cmd/C30587A9A562FF93FF2F350F875ED55F.historized.dump.xml", "");
        assertThat(taxonNode.get("taxonomicNameId").asText(), is("8CAC4D3CA562FF93FF2F350F80EFD68D"));
    }


    public JsonNode assertBarbastella(String resourceName, String taxonomicNameIdExpected) throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource(resourceName);
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

        JsonNode taxonNode = retrieveFirstJson(blobStore);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(),
                is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/plazi+xml"));
        assertThat(taxonNode.get("docId").asText(), is("C30587A9A562FF93FF2F350F875ED55F"));
        assertThat(taxonNode.get("verbatimText").asText(), is("Barbastella leucomelas (Cretzschmar, 1826) . In Ruppell, Atlas Reise Nordl. Afr., Zool. Säugeth., p. 73 . TYPE LOCALITY: Egypt , Sinai . DISTRIBUTION: Caucasus to The Pamirs, N Iran , Afganistan , India , and W China ; Honshu, Hokkaido ( Japan ); Sinai ( Egypt ); N Ethiopia ; perhaps Indo-China. SYNONYMS: blanfordi , caspica, darjelingensis , walteri."));
        assertThat(taxonNode.get("interpretedGenus"), is(nullValue()));
        assertThat(taxonNode.get("interpretedSpecies"), is(nullValue()));
        return taxonNode;
    }

    @Test
    public void streamPeropteryxPallidopteraTreatmentFromXML() throws IOException {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                URL resource = getClass().getResource("/bio/guoda/preston/cmd/03D587F2FFDB4C11F8EE3BD3FE17F84D.xml");
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

        JsonNode taxonNode = retrieveFirstJson(blobStore);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(),
                is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/plazi+xml"));
        assertThat(taxonNode.get("docId").asText(), is("03D587F2FFDB4C11F8EE3BD3FE17F84D"));
        assertThat(taxonNode.get("interpretedGenus").asText(), is("Peropteryx"));
        assertThat(taxonNode.get("interpretedSpecies").asText(), is("pallidoptera"));
        assertThat(taxonNode.get("foodAndFeeding").asText(), is("Pale-winged Dog-like Bats are insectivorous. They are attracted to mineral seeps (salt licks) perhaps to drink mineral rich waters."));

    }

    private void assertTreatmentValues(JsonNode taxonNode) {
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/plazi+xml"));
        assertThat(taxonNode.get("docId").asText(), is("03D587F2FFC94C03F8F13AECFBD8F765"));
        assertThat(taxonNode.get("interpretedGenus").asText(), is("Taphozous"));
        assertThat(taxonNode.get("interpretedSpecies").asText(), is("troughtoni"));
    }

    private JsonNode retrieveFirstJson(BlobStoreReadOnly blobStore) throws IOException {
        Quad statement = toStatement(
                toIRI("blip"),
                HAS_VERSION,
                toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1")
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        PlaziTreatmentExtractor dwcRecordExtractor = new PlaziTreatmentExtractor(
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
        assertThat(jsonObjects.length, is(1));

        return new ObjectMapper().readTree(jsonObjects[0]);
    }

}