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
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;

public class TaxoDrosFileExtractorTest {

    @Test
    public void streamTaxoDrosToLineJson() throws IOException {
        assertAssumptions("DROS5.TEXT.example.txt");

    }

    @Test
    public void streamTaxoDrosToLineJsonWithIncomplete() throws IOException {
        assertAssumptions("DROS5.TEXT.incomplete.txt");
    }

    @Test
    public void streamTaxoDrosToLineJsonWithMultilineFilename() throws IOException {
        assertAssumptions("DROS5.TEXT.longfilename.txt");
    }

    private void assertAssumptions(String testResource) throws IOException {
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
        TaxoDrosFileExtractor extractor = new TaxoDrosFileExtractor(
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
        assertThat(jsonObjects.length, is(3));

        JsonNode taxonNode = new ObjectMapper().readTree(jsonObjects[0]);

        assertThat(taxonNode.get("http://www.w3.org/ns/prov#wasDerivedFrom").asText(), is("line:hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1!/L1-L10"));
        assertThat(taxonNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("taxodros-flatfile"));
        assertThat(taxonNode.get("authors").asText(), is("Abd El-Halim, A.S., Mostafa, A.A., & Allam, K.A.M.a.,"));
        assertThat(taxonNode.get("title").asText(), is("Dipterous flies species and their densities in fourteen Egyptian governorates."));
        assertThat(taxonNode.get("journal").asText(), is("J. Egypt. Soc. Parasitol., 35:351-362."));
        assertThat(taxonNode.get("year").asText(), is("2005"));
        assertThat(taxonNode.get("method").asText(), is("ocr"));
        assertThat(taxonNode.get("filename").asText(), is("Abd El-Halim et al., 2005M.pdf"));
    }


}
