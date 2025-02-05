package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import static bio.guoda.preston.MimeTypes.MIME_TYPE_JSON;
import static bio.guoda.preston.RefNodeConstants.CREATED_BY;
import static bio.guoda.preston.RefNodeConstants.HAS_FORMAT;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.store.TestUtil.getTestPersistence;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertTrue;

public class RegistryReaderOAITest {

    @Test
    public void onMalformedResponse() throws IOException {
        String testResource = "oai/zenodo-malformed-response.xml";
        ArrayList<Quad> nodes = getStatements(testResource);
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onResponseWithResumptionToken() throws IOException {
        String testResource = "oai/zenodo-response.xml";
        ArrayList<Quad> nodes = getStatements(testResource);
        assertThat(nodes.size(), is(3));
        assertThat(getVersionSource(nodes.get(2)).getIRIString(), is("https://zenodo.org/oai2d?verb=ListRecords&resumptionToken=.eJyNjlFvgjAcxL_L_5ktFJwTEh8wjC5kMKnaQl9MtVUYVQyUyWb87tPsfdk93l3ufhfolJLg24_j0WSCxrbtud7oeWwjC05ir8B3LKjPot134F9uZQM-9J1qH1SjwYKDMkIKI-at2lXDLWtEtb4b28oouFrQbdtG63V1-4AI675wBsQx3RWO10tMRzLykGS6Do8R4nn8RHGpNzXXEqdNMHCdODpLXuPFRpO2YOS4WvFc5MQtGJoFd30GsX2qmahjxvB5UB_lYhXGtqJkR78TQ1jqyoEfk5o4WV6yrY74G_WKNCSGYEpSKt3fneR9Oc8Y_0JuuuR9dvCWxNEz9mIydSiLHDVDRv7Ls_-b5zydwvUHUiV78A.Z6Kj9Q.UuYPFAxcPkUpIokwgI80MU7QcLw"));
    }

    @Test
    public void onResponseEmptyResumptionToken() throws IOException {
        ArrayList<Quad> nodes = getStatements("oai/zenodo-response-empty-resumption.xml");
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onResponseWithoutResumptionToken() throws IOException {
        ArrayList<Quad> nodes = getStatements("oai/zenodo-response-no-resumption.xml");
        assertThat(nodes.size(), is(0));
    }


    private ArrayList<Quad> getStatements(String testResource) throws IOException {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreAppendOnly blobStore = new BlobStoreAppendOnly(getTestPersistence(), true, HashType.sha256);

        IRI responseId = blobStore.put(getClass().getResourceAsStream(testResource));

        ProcessorReadOnly registryReader = new RegistryReaderOAI(blobStore, TestUtilForProcessor.testListener(nodes));

        registryReader.on(toStatement(toIRI("https://zenodo.org/oai2d?verb=ListRecords&metadataPrefix=oai_datacite&set=user-eol"),
                HAS_VERSION,
                responseId));
        return nodes;
    }

}