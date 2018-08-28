package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefNodeType;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderGBIFTest {

    @Test
    public void parseDatasets() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("gbifdatasets.json");

        final List<RefNode> refNodes = new ArrayList<>();

        RegistryReaderGBIF.parse(resourceAsStream, refNodes::add, new RefNodeString(RefNodeType.UUID, "description"));

        assertThat(refNodes.size(), is(18));

        RefNode refNode = refNodes.get(0);
        assertThat(refNode.getType(), is(RefNodeType.UUID));
        assertThat(refNode.getLabel(), is("6555005d-4594-4a3e-be33-c70e587b63d7"));

        refNode = refNodes.get(1);
        assertThat(refNode.getType(), is(RefNodeType.RELATION));
        assertThat(refNode.getLabel(), is("[description]<-[:http://example.org/partOf]-[6555005d-4594-4a3e-be33-c70e587b63d7]"));

        refNode = refNodes.get(2);
        assertThat(refNode.getType(), is(RefNodeType.URI));
        assertThat(refNode.getLabel(), is("http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

        refNode = refNodes.get(3);
        assertThat(refNode.getType(), is(RefNodeType.RELATION));
        assertThat(refNode.getLabel(), is("[6555005d-4594-4a3e-be33-c70e587b63d7]<-[:http://example.org/partOf]-[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        RefNode thirdRefNode = refNodes.get(4);
        assertThat(thirdRefNode.getType(), is(RefNodeType.DWCA));
        assertThat(thirdRefNode.getLabel(), is("data@http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

        thirdRefNode = refNodes.get(5);
        assertThat(thirdRefNode.getType(), is(RefNodeType.URI));
        assertThat(thirdRefNode.getLabel(), is("http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

        RefNode lastRefNode = refNodes.get(refNodes.size() - 2);
        assertThat(lastRefNode.getType(), is(RefNodeType.URI));
        assertThat(lastRefNode.getLabel(), is("https://api.gbif.org/v1/dataset?offset=2&limit=2"));

        lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.getType(), is(RefNodeType.RELATION));
        assertThat(lastRefNode.getLabel(), is("[description]<-[:http://example.org/partOf]-[https://api.gbif.org/v1/dataset?offset=2&limit=2]"));

    }


}