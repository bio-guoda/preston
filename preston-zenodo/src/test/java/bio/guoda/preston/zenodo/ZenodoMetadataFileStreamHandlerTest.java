package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.Dereferencer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.TreeMap;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertNotNull;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ZenodoMetadataFileStreamHandlerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void allowedDefinedPubDate() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void allowedEmptyPubDate() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit-no-pubdate.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(true);
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void nonEmptyPubDateAllowed() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void updateResourceTypeArticle() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        String resourceType = "bla";
        assertThat(ZenodoMetadataFileStreamHandler.updateResourceType(resourceType, jsonNode), Is.is("publication"));
    }

    @Test
    public void updateResourceTypePhoto() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("zenodo-photo-deposit.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        String resourceType = "bla";
        assertThat(ZenodoMetadataFileStreamHandler.updateResourceType(resourceType, jsonNode), Is.is("image-photo"));
    }

    @Test
    public void emptyPubDateNotAllowed() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit-no-pubdate.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        assertFalse(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void emptyPubDateNotAllowed2() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("batlit-data/example-batlit-no-pubdate.json"));

        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        assertFalse(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void noFilenameAttribute() throws IOException {
        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("zenodo-metadata-globi-review.json"));
        assertNotNull(jsonNode);
        ZenodoContext contextDefault = new ZenodoContext("bla");
        contextDefault.setAllowEmptyPublicationDate(false);
        assertTrue(ZenodoMetadataFileStreamHandler.hasAllowedPublicationDate(jsonNode, contextDefault));
    }

    @Test
    public void nameForVersion() throws IOException {
        Quad versionStatement = RefNodeFactory.toStatement(RefNodeFactory.toIRI("https://example.org/file.txt"), RefNodeConstants.HAS_VERSION, RefNodeFactory.toIRI("hash://md5/b1946ac92492d2347c6235b4d2611184"));
        String filename = ZenodoMetadataFileStreamHandler.filenameFor(versionStatement);
        assertThat(filename, Is.is("file.txt"));
    }

    @Test
    public void hashNameForVersion() throws IOException {
        Quad versionStatement = RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.HAS_VERSION, RefNodeFactory.toIRI("hash://md5/b1946ac92492d2347c6235b4d2611184"));
        String filename = ZenodoMetadataFileStreamHandler.filenameFor(versionStatement);
        assertThat(filename, Is.is("b1946ac92492d2347c6235b4d2611184"));
    }

    @Test
    public void noNameForNonVersion() throws IOException {
        Quad versionStatement = RefNodeFactory.toStatement(RefNodeFactory.toIRI("https://example.org/file.txt"), RefNodeConstants.WAS_DERIVED_FROM, RefNodeFactory.toIRI("hash://md5/b1946ac92492d2347c6235b4d2611184"));
        String filename = ZenodoMetadataFileStreamHandler.filenameFor(versionStatement);
        assertThat(filename, Is.is("file.txt"));
    }

    @Test
    public void licenseForDeposit() throws IOException {
        String partUrn = "urn:lsid:biodiversitylibrary.org:part:332157";
        IRI translatorMapIRI = toIRI("foo:bar");
        final Map<String, String> licenseMap = new TreeMap<>();
        Dereferencer<InputStream> dereferencer = withDummyTranslatorMap(partUrn, translatorMapIRI);

        ZenodoMetadataFileStreamHandler.buildTranslatorMap(dereferencer, translatorMapIRI, licenseMap);

        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("/bio/guoda/preston/zenodo/zenodo-bhl-metadata.json"));

        ZenodoMetadataFileStreamHandler.annotateLicenseByAlternateIdentifier(jsonNode, licenseMap);

        JsonNode license = jsonNode.at("/metadata/license");

        assertThat(license.isMissingNode(), Is.is(false));

        assertThat(license.asText(), Is.is("cc-by-nc-sa-3.0"));

    }
    @Test
    public void noLicenseForDeposit() throws IOException {
        String partUrn = "urn:lsid:biodiversitylibrary.org:part:666";
        IRI translatorMapIRI = toIRI("foo:bar");
        final Map<String, String> licenseMap = new TreeMap<>();
        Dereferencer<InputStream> dereferencer = withDummyTranslatorMap(partUrn, translatorMapIRI);

        ZenodoMetadataFileStreamHandler.buildTranslatorMap(dereferencer, translatorMapIRI, licenseMap);

        JsonNode jsonNode = new ObjectMapper().readTree(getClass().getResourceAsStream("/bio/guoda/preston/zenodo/zenodo-bhl-metadata.json"));

        ZenodoMetadataFileStreamHandler.annotateLicenseByAlternateIdentifier(jsonNode, licenseMap);

        JsonNode license = jsonNode.at("/metadata/license");

        assertThat(license.isMissingNode(), Is.is(true));

    }

    private Dereferencer<InputStream> withDummyTranslatorMap(String partUrn, final IRI licenseMapIRIProvided) throws IOException {
        Quad versionStatement = RefNodeFactory.toStatement(
                RefNodeFactory.toIRI(partUrn),
                RefNodeConstants.HAS_LICENSE,
                RefNodeFactory.toIRI("https://spdx.org/licenses/CC-BY-NC-SA-3.0")
        );

        File file = folder.newFile("license-map.nq");
        try (FileOutputStream outputStream = new FileOutputStream(file)) {
            IOUtils.copy(IOUtils.toInputStream(versionStatement.toString(), StandardCharsets.UTF_8), outputStream);
        }

        Dereferencer<InputStream> dereferencer = new Dereferencer<InputStream>() {

            @Override
            public InputStream get(IRI licenseMapIRI) throws IOException {
                if (!licenseMapIRIProvided.equals(licenseMapIRI)) {
                    throw new IOException("kaboom!");
                }
                return Files.newInputStream(file.toPath());
            }
        };
        return dereferencer;
    }

}