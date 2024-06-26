package bio.guoda.preston.cmd;

import bio.guoda.preston.process.ProcessorStateAlwaysContinue;
import bio.guoda.preston.store.BlobStoreReadOnly;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RegExUtils;
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

import static bio.guoda.preston.RefNodeConstants.HAS_TYPE;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_DERIVED_FROM;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static junit.framework.TestCase.assertNull;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;

public class GitHubJSONExtractorTest {

    @Test
    public void latestIssueJSON() throws IOException {
        assertLineJSONResult("/bio/guoda/preston/process/github/latest_issue.json", 1);
    }

    @Test
    public void singleIssueJSON() throws IOException {
        assertLineJSONResult("/bio/guoda/preston/process/github/904_issue.json", 1);
    }

    @Test
    public void singleIssueCommentJSON() throws IOException {
        assertLineJSONResult("/bio/guoda/preston/process/github/904_issue_comments.json", 18);
    }

    @Test
    public void nonProcessingInvalidIssueJSON() throws IOException {
        String actual = processResource("/bio/guoda/preston/process/github/invalid_issue.txt");
        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(0));
    }

    @Test
    public void nonGitHubJSON() throws IOException {
        String actual = processResource("/bio/guoda/preston/process/github/invalid_issue.json");
        String[] jsonObjects = StringUtils.split(actual, "\n");
        assertThat(jsonObjects.length, is(0));
    }

    private void assertLineJSONResult(String resourceName, int expectedNumberOfObjects) throws IOException {
        String actual = processResource(resourceName);

        String[] actualJSONs = StringUtils.split(actual, "\n");
        assertThat(actualJSONs.length, is(expectedNumberOfObjects));

        JsonNode expectedJSON = new ObjectMapper().readTree(getClass().getResourceAsStream(resourceName));

        assertThat(actualJSONs[0], not(is(IOUtils.toString(getClass().getResourceAsStream(resourceName), StandardCharsets.UTF_8))));
        String expectedJSONString = expectedJSON.isArray() ? expectedJSON.get(0).toString() : expectedJSON.toString();

        assertThat(actualJSONs[0], is(RegExUtils.replacePattern(expectedJSONString, "}$", ",\"http://www.w3.org/ns/prov#wasDerivedFrom\":\"hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1\",\"http://www.w3.org/1999/02/22-rdf-syntax-ns#type\":\"application/vnd.github+json\"}")));

        JsonNode actualNode = new ObjectMapper().readTree(actualJSONs[0]);
        assertFalse(actualNode.isArray());
        assertThat(actualNode.get(WAS_DERIVED_FROM.getIRIString()).asText(), is("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1"));
        assertThat(actualNode.get("http://www.w3.org/1999/02/22-rdf-syntax-ns#type").asText(), is("application/vnd.github+json"));


    }

    private String processResource(String resourceName) throws IOException {
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

        Quad statement = toStatement(
                toIRI("blip"),
                HAS_VERSION,
                toIRI("hash://sha256/856ecd48436bb220a80f0a746f94abd7c4ea47cb61d946286f7e25cf0ec69dc1")
        );

        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        GitHubJSONExtractor extractor = new GitHubJSONExtractor(
                new ProcessorStateAlwaysContinue(),
                blobStore,
                byteArrayOutputStream
        );

        extractor.on(statement);

        return IOUtils.toString(
                byteArrayOutputStream.toByteArray(),
                StandardCharsets.UTF_8.name()
        );
    }


}
