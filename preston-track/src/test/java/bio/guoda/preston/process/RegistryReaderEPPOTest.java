package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class RegistryReaderEPPOTest {

    public static final String EPPO_DATASETS_JSON = "eppo-taxons.json";

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementsListener adapt = TestUtilForProcessor.testListener(nodes);
        RegistryReaderEPPO registryReader = new RegistryReaderEPPO(TestUtil.getTestBlobStore(HashType.sha256), adapt);
        registryReader.on(toStatement(Seeds.OBIS, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        assertThat(nodes.size(), is(6));
        assertThat(getVersionSource(nodes.get(5)).getIRIString(), is("https://api.eppo.int/gd/v2/taxons/list?limit=1000"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderOBIS registryReader = new RegistryReaderOBIS(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));

        registryReader.on(toStatement(toIRI("https://api.gbif.org/v1/dataset"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderOBIS registryReader = new RegistryReaderOBIS(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.GBIF, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void parseTaxonsSinglePage() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        IRI testNode = createTestNode();

        RegistryReaderEPPO.parse(testNode, TestUtilForProcessor.testEmitter(refNodes), getClass().getResourceAsStream(EPPO_DATASETS_JSON));

        assertThat(refNodes.size(), is(3));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <https://api.eppo.int/gd/v2/taxons/taxon/BEMITA/overview> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<https://api.eppo.int/gd/v2/taxons/taxon/BEMITA/overview> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), startsWith("<https://api.eppo.int/gd/v2/taxons/taxon/BEMITA/overview> <http://purl.org/pav/hasVersion> "));
    }

    @Test
    public void parseTaxonsManyPage() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        IRI testNode = createTestNode("eppo-taxons-20260925.json");

        RegistryReaderEPPO.parse(testNode,
                TestUtilForProcessor.testEmitter(refNodes),
                getClass().getResourceAsStream("eppo-taxons-20260925.json")
        );

        assertThat(refNodes.size(), is(690));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <https://api.eppo.int/gd/v2/taxons/taxon/ABSICO/overview> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<https://api.eppo.int/gd/v2/taxons/taxon/ABSICO/overview> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), startsWith("<https://api.eppo.int/gd/v2/taxons/taxon/ABSICO/overview> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(refNodes.size() - 3);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <https://api.eppo.int/gd/v2/taxons/list?limit=1000&offset=129001> ."));

        refNode = refNodes.get(refNodes.size() - 2);
        assertThat(refNode.toString(), is("<https://api.eppo.int/gd/v2/taxons/list?limit=1000&offset=129001> <http://purl.org/dc/elements/1.1/format> \"application/json\" ."));

        refNode = refNodes.get(refNodes.size() - 1);
        assertThat(refNode.toString(), startsWith("<https://api.eppo.int/gd/v2/taxons/list?limit=1000&offset=129001> <http://purl.org/pav/hasVersion> "));
    }

    private IRI createTestNode() {
        return createTestNode(EPPO_DATASETS_JSON);
    }

    private IRI createTestNode(String eppoDatasetsJson) {
        try {
            return toIRI(getClass().getResource(eppoDatasetsJson).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}