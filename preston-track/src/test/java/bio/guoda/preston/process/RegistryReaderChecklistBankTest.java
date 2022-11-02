package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.Seeds;
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
import static bio.guoda.preston.RefNodeConstants.SEE_ALSO;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;
import static org.junit.Assert.assertTrue;

public class RegistryReaderChecklistBankTest {

    public static final String CHECKLIST_BANK_DATASETS_JSON = "checklistbank-datasets.json";

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderChecklistBank registryReader = new RegistryReaderChecklistBank(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        registryReader.on(toStatement(Seeds.CHECKLIST_BANK, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        assertThat(new HashSet<>(nodes).size(), is(6));
        assertThat(nodes.size(), is(6));
        assertThat(getVersionSource(nodes.get(5)).getIRIString(), is("https://api.checklistbank.org/dataset"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderChecklistBank registryReader = new RegistryReaderChecklistBank(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));

        registryReader.on(toStatement(toIRI("https://api.checklistbank.org/dataset"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(1));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderChecklistBank registryReader = new RegistryReaderChecklistBank(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.CHECKLIST_BANK, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onContinuation() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream(CHECKLIST_BANK_DATASETS_JSON);
            }
        };
        RegistryReaderChecklistBank registryReader = new RegistryReaderChecklistBank(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://api.checklistbank.org/dataset"), HAS_VERSION, createTestNode());

        registryReader.on(firstPage);

        assertThat(nodes.size(), is(13168));
        assertThat(nodes.get(2), hasTriple(toStatement(toIRI("https://api.checklistbank.org/dataset/3"), SEE_ALSO, toIRI("https://doi.org/10.48580/ds5"))));

        Quad lastPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(lastPage).toString(), is("<https://api.checklistbank.org/dataset?offset=43680&limit=10>"));
    }

    @Test
    public void nextPage() {
        List<Quad> nodes = new ArrayList<>();
        RegistryReaderChecklistBank.emitNextPage(0, 10, TestUtilForProcessor.testEmitter(nodes), "https://bla/?limit=2&offset=8");
        assertThat(nodes.size(), is(3));
        assertThat(nodes.get(1).getSubject().toString(), is("<https://bla/?limit=10&offset=0>"));
    }

    @Test
    public void parseDatasets() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        IRI testNode = createTestNode();

        RegistryReaderChecklistBank.parseDatasetResultPage(testNode,
                TestUtilForProcessor.testEmitter(refNodes),
                getClass().getResourceAsStream(CHECKLIST_BANK_DATASETS_JSON),
                toIRI("http://example.org/"));

        assertThat(refNodes.size(), is(13167));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <https://api.checklistbank.org/dataset/3> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<https://api.checklistbank.org/dataset/3> <http://www.w3.org/1999/02/22-rdf-syntax-ns#seeAlso> <https://doi.org/10.48580/ds5> ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), is("<https://api.checklistbank.org/dataset/3> <http://www.w3.org/ns/prov#hadMember> <https://api.checklistbank.org/dataset/3/archive.zip> ."));

        refNode = refNodes.get(3);
        assertThat(refNode.toString(), is("<https://api.checklistbank.org/dataset/3/archive.zip> <http://purl.org/dc/elements/1.1/format> \"application/zip\" ."));

        refNode = refNodes.get(4);
        assertThat(refNode.toString(), startsWith("<https://api.checklistbank.org/dataset/3/archive.zip> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(5);
        assertThat(refNode.toString(), is("<https://api.checklistbank.org/dataset/3> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        refNode = refNodes.get(6);
        assertThat(refNode.toString(), startsWith("<https://api.checklistbank.org/dataset/3> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(7);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <https://api.checklistbank.org/dataset/1000> ."));

        refNode = refNodes.get(9);
        assertThat(refNode.toString(), is("<https://api.checklistbank.org/dataset/1000/archive.zip> <http://purl.org/dc/elements/1.1/format> \"application/zip\" ."));

        refNode = refNodes.get(10);
        assertThat(refNode.toString(), startsWith("<https://api.checklistbank.org/dataset/1000/archive.zip> <http://purl.org/pav/hasVersion> "));

        Quad lastRefNode = refNodes.get(refNodes.size() - 2);
        assertThat(lastRefNode.toString(), is("<http://example.org/?offset=43680&limit=10> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.toString(), startsWith("<http://example.org/?offset=43680&limit=10> <http://purl.org/pav/hasVersion> "));

    }


    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource("checklistbank-datasets.json").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}