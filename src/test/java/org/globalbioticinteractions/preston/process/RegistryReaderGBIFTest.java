package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.store.TestUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderGBIFTest {

    public static final String GBIFDATASETS_JSON = "gbifdatasets.json";

    @Test
    public void onSeed() {
        ArrayList<RefStatement> nodes = new ArrayList<>();
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(TestUtil.getTestBlobStore(), nodes::add);
        RefNodeString bla = new RefNodeString("bla");
        registryReaderGBIF.on(new RefStatement(Seeds.SEED_NODE_GBIF, RefNodeConstants.SEED_OF, bla));
        Assert.assertThat(nodes.size(), is(2));
        assertThat(nodes.get(1).getObject().getLabel(), is("https://api.gbif.org/v1/dataset"));
    }

    @Test
    public void onNotSeed() {
        ArrayList<RefStatement> nodes = new ArrayList<>();
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(TestUtil.getTestBlobStore(), nodes::add);
        RefNodeString bla = new RefNodeString("bla");
        registryReaderGBIF.on(new RefStatement(Seeds.SEED_NODE_GBIF, RefNodeConstants.HAD_MEMBER, bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onContinuation() {
        ArrayList<RefStatement> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(URI key) throws IOException {
                return getClass().getResourceAsStream(GBIFDATASETS_JSON);
            }
        };
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(blobStore, nodes::add);


        RefStatement firstPage = new RefStatement(createTestNode(), RefNodeConstants.WAS_DERIVED_FROM, new RefNodeString("https://api.gbif.org/v1/dataset"));

        registryReaderGBIF.on(firstPage);

        Assert.assertThat(nodes.size(), is(18));
        RefStatement secondPage = nodes.get(nodes.size() - 1);
        assertThat(secondPage.getObject().getLabel(), is("https://api.gbif.org/v1/dataset?offset=2&limit=2"));
    }

    @Test
    public void parseDatasets() throws IOException {

        final List<RefStatement> refNodes = new ArrayList<>();

        RefNode testNode = createTestNode();

        RegistryReaderGBIF.parse(testNode, refNodes::add, new RefNodeString("description"), getClass().getResourceAsStream(GBIFDATASETS_JSON));

        assertThat(refNodes.size(), is(18));

        RefStatement refNode = refNodes.get(0);
        assertThat(refNode.getLabel(), is("[https://gbif.org]-[:http://www.w3.org/ns/prov#hadMember]->[label@gbifdatasets.json]"));

        refNode = refNodes.get(1);
        assertThat(refNode.getLabel(), is("[label@gbifdatasets.json]-[:http://www.w3.org/ns/prov#hadMember]->[6555005d-4594-4a3e-be33-c70e587b63d7]"));

        refNode = refNodes.get(2);
        assertThat(refNode.getLabel(), is("[6555005d-4594-4a3e-be33-c70e587b63d7]-[:http://www.w3.org/ns/prov#hadMember]->[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        refNode = refNodes.get(3);
        assertThat(refNode.getLabel(), is("[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]-[:http://purl.org/dc/elements/1.1/format]->[application/dwca]"));

        refNode = refNodes.get(4);
        assertThat(refNode.getLabel(), is("[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        refNode = refNodes.get(5);
        assertThat(refNode.getLabel(), is("[6555005d-4594-4a3e-be33-c70e587b63d7]-[:http://www.w3.org/ns/prov#hadMember]->[http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        refNode = refNodes.get(6);
        assertThat(refNode.getLabel(), is("[http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]-[:http://purl.org/dc/elements/1.1/format]->[application/eml]"));

        refNode = refNodes.get(7);
        assertThat(refNode.getLabel(), is("[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        refNode = refNodes.get(8);
        assertThat(refNode.getLabel(), is("[label@gbifdatasets.json]-[:http://www.w3.org/ns/prov#hadMember]->[d0df772d-78f4-4602-acf2-7d768798f632]"));

        RefStatement lastRefNode = refNodes.get(refNodes.size() - 3);
        assertThat(lastRefNode.getLabel(), is("[https://api.gbif.org/v1/dataset?offset=2&limit=2]-[:http://example.org/continuationOf]->[description]"));

        lastRefNode = refNodes.get(refNodes.size() - 2);
        assertThat(lastRefNode.getLabel(), is("[https://api.gbif.org/v1/dataset?offset=2&limit=2]-[:http://purl.org/dc/elements/1.1/format]->[application/json]"));

        lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.getLabel(), is("[?]-[:http://www.w3.org/ns/prov#wasDerivedFrom]->[https://api.gbif.org/v1/dataset?offset=2&limit=2]"));

    }

    private RefNode createTestNode() {
        return new RefNodeFromResource();
    }


}