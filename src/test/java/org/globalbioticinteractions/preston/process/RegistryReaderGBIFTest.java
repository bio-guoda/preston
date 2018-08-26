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
import static org.junit.Assert.*;

public class RegistryReaderGBIFTest {

    @Test
    public void parseDatasets() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("gbifdatasets.json");

        final List<RefNode> refNodes = new ArrayList<>();

        RegistryReaderGBIF.parse(resourceAsStream, refNodes::add, new RefNodeString(null, RefNodeType.UUID, "description"));

        assertThat(refNodes.size(), is(12));
        RefNode refNode = refNodes.get(0);
        assertThat(refNode.getType(), is(RefNodeType.UUID));
        assertThat(refNode.getLabel(), is("6555005d-4594-4a3e-be33-c70e587b63d7"));

        RefNode lastRefNode = refNodes.get(3);
        assertThat(lastRefNode.getType(), is(RefNodeType.URI));
        assertThat(lastRefNode.getLabel(), is("http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06"));

    }


}