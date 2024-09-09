package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.stream.ContentStreamException;
import bio.guoda.preston.zenodo.ZenodoConfig;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsCollectionContaining.hasItem;

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
                byteArrayOutputStream,
                testConfig(),
                new PublicationDateFactory() {
                    @Override
                    public String getPublicationDate() {
                        return "2022-01-02";
                    }
                }
        );

        extractor.on(statement);

        String actual = IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );

        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(80));

        JsonNode taxonNode = unwrapMetadata(jsonObjects[0]);


        assertDescription(taxonNode);

        assertThat(taxonNode.get("isDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L9"));

        assertThat(taxonNode.at("/publication_date").asText(), is("2022-01-02"));
        assertThat(taxonNode.at("/creators/0/name").asText(), is("Museum für Naturkunde Berlin"));
        assertThat(taxonNode.at("/communities/0/identifier").asText(), is("my-community"));

        JsonNode identifiers = taxonNode.at("/related_identifiers");
        assertThat(identifiers.size(), is(5));
        // provided by README
        assertThat(identifiers.get(0).get("relation").asText(), is("isDerivedFrom"));
        assertThat(identifiers.get(0).get("identifier").asText(), is("https://linker.bio/line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L9"));
        // constructed from institution code (mfn)
        assertThat(identifiers.get(1).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(1).get("identifier").asText(), is("hash://sha256/72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822"));

        assertThat(identifiers.get(2).get("relation").asText(), is("hasVersion"));
        assertThat(identifiers.get(2).get("identifier").asText(), is("hash://sha256/72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822"));

        // lsid to disambiguate records
        assertThat(identifiers.get(3).get("relation").asText(), is("isAlternateIdentifier"));
        assertThat(identifiers.get(3).get("identifier").asText(), is("urn:lsid:github.com:darktaxon:BMT0009397:BMT121_BMT0009397_RAW_01_01.tiff"));

        // lsid to disambiguate records
        assertThat(identifiers.get(4).get("relation").asText(), is("documents"));
        assertThat(identifiers.get(4).get("identifier").asText(), is("urn:lsid:github.com:darktaxon:BMT0009397"));

        JsonNode references = taxonNode.at("/references");
        assertThat(references.size(), is(2));
        assertThat(references.get(0).asText(), is("Hartop E, Srivathsan A, Ronquist F, Meier R (2022) Towards Large-Scale Integrative Taxonomy (LIT): resolving the data conundrum for dark taxa. Syst Biol 71:1404–1422. https://doi.org/10.1093/sysbio/syac033"));
        assertThat(references.get(1).asText(), is("Srivathsan, A., Meier, R. (2024). Scalable, Cost-Effective, and Decentralized DNA Barcoding with Oxford Nanopore Sequencing. In: DeSalle, R. (eds) DNA Barcoding. Methods in Molecular Biology, vol 2744. Humana, New York, NY. https://doi.org/10.1007/978-1-0716-3581-0_14"));



        assertThat(taxonNode.get("darktaxon:plateId").asText(), is("BMT121"));
        assertThat(taxonNode.get("darktaxon:specimenId").asText(), is("BMT0009397"));
        assertThat(taxonNode.get("darktaxon:imageFilepath").asText(), is("BMT121/BMT0009397/BMT121_BMT0009397_RAW_Data_01/BMT121_BMT0009397_RAW_01_01.tiff"));
        assertThat(taxonNode.get("darktaxon:imageStackNumber").asText(), is("01"));
        assertThat(taxonNode.get("darktaxon:imageAcquisitionMethod").asText(), is("RAW"));
        assertThat(taxonNode.get("darktaxon:imageNumber").asText(), is("01"));
        assertThat(taxonNode.get("darktaxon:imageContentId").asText(), is("hash://sha256/72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822"));
        assertThat(taxonNode.get("darktaxon:mimeType").asText(), is("image/tiff"));


        taxonNode = unwrapMetadata(jsonObjects[jsonObjects.length - 1 - 10]);


        assertDescription(taxonNode);

        assertThat(taxonNode.get("isDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L78"));
        assertThat(taxonNode.get("title").asText(), is("Photo of Specimen BMT0009392"));
        assertThat(taxonNode.get("darktaxon:plateId").asText(), is("BMT121"));
        assertThat(taxonNode.get("darktaxon:specimenId").asText(), is("BMT0009392"));
        assertThat(taxonNode.get("filename").asText(), is("BMT121_BMT0009392_stacked_04.tiff"));
        assertThat(taxonNode.get("darktaxon:imageFilepath").asText(), is("BMT121/BMT0009392/BMT121_BMT0009392_stacked_04.tiff"));
        assertThat(taxonNode.get("darktaxon:imageStackNumber").asText(), is("04"));
        assertThat(taxonNode.get("darktaxon:imageNumber"), is(nullValue()));
        assertThat(taxonNode.get("darktaxon:imageAcquisitionMethod").asText(), is("stacked"));
        assertThat(taxonNode.get("darktaxon:mimeType").asText(), is("image/tiff"));
        assertThat(taxonNode.get("description").asText(), is("Uploaded by Plazi for the Museum für Naturkunde Berlin."));
        assertThat(taxonNode.get("upload_type").asText(), is("image"));
        assertThat(taxonNode.get("image_type").asText(), is("photo"));

        taxonNode = unwrapMetadata(jsonObjects[jsonObjects.length - 1]);


        assertDescription(taxonNode);

        JsonNode jsonNode = taxonNode.get("isDerivedFrom");
        assertThat(jsonNode.isArray(), is(true));

        ArrayNode arrayNode = (ArrayNode) jsonNode;
        List<String> contentIds = new ArrayList<>();
        arrayNode.forEach(id -> contentIds.add(id.asText()));

        assertThat(taxonNode.get("darktaxon:imageContentId").asText(), is("hash://sha256/0ef95afa1cb5b7a343a49da4222b57d87f9a7afa20f37b91e4924dfc490db646"));

        assertThat(contentIds.size(), is(6));

        assertThat(contentIds, hasItem("hash://sha256/72a63d47805f78e4529ec282e3e8e8412beb456e571c1e2276a107b3f0fa9822"));
        assertThat(contentIds, hasItem("hash://sha256/ec550e7b16986b8ca2957478560001c22072c5f7c350fb3de7c90ec21f848d99"));
        assertThat(contentIds, hasItem("hash://sha256/0bb85801fba1dd60365e481c92e7dedbf5d13af00e922f2af2cd1761b144822b"));
        assertThat(contentIds, hasItem("hash://sha256/e207b334d0132c9f734de5948bd58cf864a8c325055fd0498fc0fee3d54af65e"));
        assertThat(contentIds, hasItem("hash://sha256/1ff3fd9a4d4d66189f59c27dcbd86e1909ce0c45ed2df04a209109af1e40a4a6"));
        assertThat(contentIds, hasItem("hash://sha256/fa2655a77e1167a1b568f24b1fc4c12d9f1431b9fa1033a56d24e73d235e4128"));
    }

    private ZenodoConfig testConfig() {
        return new ZenodoConfig() {

            @Override
            public String getAccessToken() {
                return null;
            }

            @Override
            public String getEndpoint() {
                return null;
            }

            @Override
            public List<String> getCommunities() {
                return Arrays.asList("my-community");
            }

            @Override
            public void setCreateNewVersionForExisting(Boolean skipOnExisting) {

            }

            @Override
            public boolean createNewVersionForExisting() {
                return false;
            }

            @Override
            public void setPublishRestrictedOnly(boolean restrictedOnly) {

            }

            @Override
            public boolean shouldPublishRestrictedOnly() {
                return false;
            }

            @Override
            public void setUpdateMetadataOnly(boolean updateMetadataOnly) {

            }

            @Override
            public boolean shouldUpdateMetadataOnly() {
                return false;
            }

            @Override
            public void setAllowEmptyPublicationDate(boolean allowEmptyPublicationDate) {

            }

            @Override
            public boolean shouldAllowEmptyPublicationDate() {
                return false;
            }
        };
    }


    private void assertDescription(JsonNode taxonNode) {
        assertThat(taxonNode.get("description").asText(), is("Uploaded by Plazi for the Museum für Naturkunde Berlin."));
    }

    private JsonNode unwrapMetadata(String jsonObject) throws JsonProcessingException {
        JsonNode rootNode = new ObjectMapper().readTree(jsonObject);
        return rootNode.get("metadata");
    }

}
