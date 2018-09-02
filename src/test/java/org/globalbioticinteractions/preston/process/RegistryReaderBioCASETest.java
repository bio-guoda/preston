package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.RefNodeConstants;
import org.globalbioticinteractions.preston.Seeds;
import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.model.RefNodeString;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.globalbioticinteractions.preston.store.TestUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

public class RegistryReaderBioCASETest {

    private ArrayList<RefStatement> nodes;
    private RefStatementListener registryReader;

    @Before
    public void init() {
        nodes = new ArrayList<>();
        registryReader = new RegistryReaderBioCASE(TestUtil.getTestBlobStore(), nodes::add);
    }

    @Test
    public void onSeed() {
        RefNode bla = RefNodeFactory.toLiteral("bla");
        registryReader.on(new RefStatement(Seeds.SEED_NODE_BIOCASE, RefNodeConstants.SEED_OF, bla));
        Assert.assertThat(nodes.size(), is(2));
        assertThat(nodes.get(1).getObject().getLabel(), is(RegistryReaderBioCASE.BIOCASE_REGISTRY_ENDPOINT));
    }

    @Test
    public void parseDataAccessURL() {
        URI generated = RegistryReaderBioCASE.generateDataSourceAccessUrl("https://bla", "boo");
        assertThat(generated.toString(), is("https://bla/pywrapper.cgi?dsa=boo&inventory=1"));
    }

    @Test
    public void parseDataAccessURL2() {
        URI generated = RegistryReaderBioCASE.generateDataSourceAccessUrl("https://bla/", "boo");
        assertThat(generated.toString(), is("https://bla/pywrapper.cgi?dsa=boo&inventory=1"));
    }

    @Test
    public void parseIllegalDataAccessURL() {
        URI generated = RegistryReaderBioCASE.generateDataSourceAccessUrl("::!@#$", "boo");
        assertNull(generated);
    }

    @Test
    public void parseProviders() throws IOException {
        InputStream providers = getClass().getResourceAsStream("biocase-providers.json");

        RegistryReaderBioCASE.parseProviders(providers, nodes::add);

        assertFalse(nodes.isEmpty());
        assertThat(nodes.get(1).getObject().getLabel(), is("http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1"));
    }

    @Test
    public void handleProviders() throws IOException {
        RefNodeFromResource refNode = new RefNodeFromResource("biocase-providers.json");

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {

            @Override
            public InputStream get(URI key) throws IOException {
                return getClass().getResourceAsStream("biocase-providers.json");
            }
        };
        registryReader = new RegistryReaderBioCASE(blobStore, nodes::add);
        registryReader.on(new RefStatement(refNode, RefNodeConstants.WAS_DERIVED_FROM, RefNodeFactory.toURI(RegistryReaderBioCASE.BIOCASE_REGISTRY_ENDPOINT)));

        assertFalse(nodes.isEmpty());
        assertThat(nodes.get(1).getObject().getLabel(), is("http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1"));
    }


    @Test
    public void parseDatasets() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        InputStream datasets = getClass().getResourceAsStream("biocase-datasets.xml");

        RegistryReaderBioCASE.parseDatasetInventory(datasets, nodes::add);

        assertThat(nodes.size(), is(2));
        assertThat(nodes.get(1).getObject().getLabel(), is("http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip"));
    }

    @Test
    public void handleDatasets() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        RefNodeFromResource refNode = new RefNodeFromResource("biocase-datasets.xml");
        registryReader = new RegistryReaderBioCASE(new BlobStoreReadOnly() {
            @Override
            public InputStream get(URI key) throws IOException {
                return getClass().getResourceAsStream("biocase-datasets.xml");
            }
        }, nodes::add);

        registryReader.on(new RefStatement(refNode, RefNodeConstants.WAS_DERIVED_FROM, RefNodeFactory.toURI("http://something/pywrapper.cgi?dsa=")));

        assertThat(nodes.size(), is(2));
        assertThat(nodes.get(1).getObject().getLabel(), is("http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip"));
    }

    @Test
    public void parseDatasetsWithDCWA() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        // example from https://wiki.bgbm.org/bps/index.php/Archiving
        InputStream datasets = getClass().getResourceAsStream("biocase-datasets-example.xml");

        RegistryReaderBioCASE.parseDatasetInventory(datasets, nodes::add);

        assertThat(nodes.size(), is(4));
        assertThat(nodes.get(0).getObject().getLabel(), is("application/abcda"));
        assertThat(nodes.get(1).getObject().getLabel(), is("http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.ABCD_2.06.zip"));
        assertThat(nodes.get(2).getObject().getLabel(), is("application/dwca"));
        assertThat(nodes.get(3).getObject().getLabel(), is("http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.DwCA.zip"));
    }


}