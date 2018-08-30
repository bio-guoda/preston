package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderGBIFTest {

    @Test
    public void onSeed() {
        ArrayList<RefStatement> nodes = new ArrayList<>();
        RegistryReaderGBIF registryReaderGBIF = new RegistryReaderGBIF(nodes::add);
        RefNodeString bla = new RefNodeString("bla");
        registryReaderGBIF.on(new RefStatement(bla, bla, Seeds.SEED_NODE_GBIF));
        Assert.assertThat(nodes.size(), is(2));
    }


    @Test
    public void parseDatasets() throws IOException {
        InputStream resourceAsStream = getClass().getResourceAsStream("gbifdatasets.json");

        final List<RefStatement> refNodes = new ArrayList<>();

        RegistryReaderGBIF.parse(resourceAsStream, refNodes::add, new RefNodeString("description"));

        assertThat(refNodes.size(), is(15));

        RefStatement refNode = refNodes.get(0);
        assertThat(refNode.getLabel(), is("[description]-[:http://example.org/hasPart]->[6555005d-4594-4a3e-be33-c70e587b63d7]"));

        refNode = refNodes.get(1);
        assertThat(refNode.getLabel(), is("[6555005d-4594-4a3e-be33-c70e587b63d7]-[:http://example.org/hasPart]->[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        refNode = refNodes.get(2);
        assertThat(refNode.getLabel(), is("[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]-[:http://purl.org/dc/elements/1.1/format]->[application/zip+dwca]"));

        refNode = refNodes.get(3);
        assertThat(refNode.getLabel(), is("[http://www.snib.mx/iptconabio/archive.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]-[:http://example.com/hasContent]->[?]"));

        refNode = refNodes.get(4);
        assertThat(refNode.getLabel(), is("[6555005d-4594-4a3e-be33-c70e587b63d7]-[:http://example.org/hasPart]->[http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]"));

        refNode = refNodes.get(5);
        assertThat(refNode.getLabel(), is("[http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]-[:http://purl.org/dc/elements/1.1/format]->[text/xml+eml]"));

        refNode = refNodes.get(6);
        assertThat(refNode.getLabel(), is("[http://www.snib.mx/iptconabio/eml.do?r=SNIB-ME006-ME0061704F-ictioplancton-CH-SIB.2017.06.06]-[:http://example.com/hasContent]->[?]"));

        refNode = refNodes.get(7);
        assertThat(refNode.getLabel(), is("[description]-[:http://example.org/hasPart]->[d0df772d-78f4-4602-acf2-7d768798f632]"));

        RefStatement lastRefNode = refNodes.get(refNodes.size() - 1);
        assertThat(lastRefNode.getLabel(), is("[description]-[:http://example.org/continuedAt]->[https://api.gbif.org/v1/dataset?offset=2&limit=2]"));

    }


}