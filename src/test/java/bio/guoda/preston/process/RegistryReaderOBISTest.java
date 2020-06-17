package bio.guoda.preston.process;

import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class RegistryReaderOBISTest {

    public static final String OBIS_DATASETS_JSON = "obis-datasets.json";

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        StatementsListener adapt = TestUtil.testListener(nodes);
        RegistryReaderOBIS registryReader = new RegistryReaderOBIS(TestUtil.getTestBlobStore(), adapt);
        registryReader.on(toStatement(Seeds.OBIS, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        Assert.assertThat(nodes.size(), is(6));
        assertThat(getVersionSource(nodes.get(5)).getIRIString(), is("https://api.obis.org/v3/dataset"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderOBIS registryReader = new RegistryReaderOBIS(TestUtil.getTestBlobStore(), TestUtil.testListener(nodes));

        registryReader.on(toStatement(toIRI("https://api.gbif.org/v1/dataset"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderOBIS registryReader = new RegistryReaderOBIS(TestUtil.getTestBlobStore(), TestUtil.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.GBIF, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void parseDatasets() throws IOException {

        final List<Quad> refNodes = new ArrayList<>();

        IRI testNode = createTestNode();

        RegistryReaderOBIS.parse(testNode, refNodes::add, getClass().getResourceAsStream(OBIS_DATASETS_JSON));

        assertThat(refNodes.size(), is(12));

        Quad refNode = refNodes.get(0);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <4354345d-7faf-4376-b326-ffbc04b6b0cd> ."));

        refNode = refNodes.get(1);
        assertThat(refNode.toString(), is("<4354345d-7faf-4376-b326-ffbc04b6b0cd> <http://www.w3.org/ns/prov#hadMember> <http://ipt.obis.org/nonode/archive.do?r=wod2009> ."));

        refNode = refNodes.get(2);
        assertThat(refNode.toString(), is("<http://ipt.obis.org/nonode/archive.do?r=wod2009> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" ."));

        refNode = refNodes.get(3);
        assertThat(refNode.toString(), startsWith("<http://ipt.obis.org/nonode/archive.do?r=wod2009> <http://purl.org/pav/hasVersion> "));

        refNode = refNodes.get(4);
        assertThat(refNode.toString(), endsWith("<http://www.w3.org/ns/prov#hadMember> <ad65221f-0539-44aa-925e-4acf62ad0c6a> ."));

        refNode = refNodes.get(5);
        assertThat(refNode.toString(), is("<ad65221f-0539-44aa-925e-4acf62ad0c6a> <http://www.w3.org/ns/prov#hadMember> <http://ipt.vliz.be/eurobis/archive.do?r=ices_datras_ns_ibts> ."));

        refNode = refNodes.get(6);
        assertThat(refNode.toString(), is("<http://ipt.vliz.be/eurobis/archive.do?r=ices_datras_ns_ibts> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" ."));

        refNode = refNodes.get(7);
        assertThat(refNode.toString(), startsWith("<http://ipt.vliz.be/eurobis/archive.do?r=ices_datras_ns_ibts> <http://purl.org/pav/hasVersion> "));

    }

    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource(OBIS_DATASETS_JSON).toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}