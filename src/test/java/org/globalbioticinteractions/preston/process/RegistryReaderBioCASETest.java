package org.globalbioticinteractions.preston.process;

import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.globalbioticinteractions.preston.Seeds;
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

import static org.globalbioticinteractions.preston.RefNodeConstants.HAS_VERSION;
import static org.globalbioticinteractions.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.getVersionSource;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toIRI;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toLiteral;
import static org.globalbioticinteractions.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class RegistryReaderBioCASETest {

    private ArrayList<Triple> nodes;
    private StatementListener registryReader;

    @Before
    public void init() {
        nodes = new ArrayList<>();
        registryReader = new RegistryReaderBioCASE(TestUtil.getTestBlobStore(), TestUtil.getTestCrawlContext(), nodes::add);
    }

    @Test
    public void onSeed() {
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.BIOCASE, WAS_ASSOCIATED_WITH, bla));
        Assert.assertThat(nodes.size(), is(4));
        assertThat(((IRI) getVersionSource(nodes.get(3))).getIRIString(), is(RegistryReaderBioCASE.BIOCASE_REGISTRY_ENDPOINT));
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
        assertThat(getVersionSource(nodes.get(1)).toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));
    }

    @Test
    public void handleProviders() throws IOException {
        IRI refNode = toIRI("https://example.org/test");

        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {

            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("biocase-providers.json");
            }
        };
        registryReader = new RegistryReaderBioCASE(blobStore, TestUtil.getTestCrawlContext(), nodes::add);
        registryReader.on(toStatement(toIRI(RegistryReaderBioCASE.BIOCASE_REGISTRY_ENDPOINT), HAS_VERSION, refNode));

        assertFalse(nodes.isEmpty());
        assertThat(nodes.get(1).getSubject().toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));
    }


    @Test
    public void parseDatasets() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        InputStream datasets = getClass().getResourceAsStream("biocase-datasets.xml");

        RegistryReaderBioCASE.parseDatasetInventory(datasets, nodes::add);

        assertThat(nodes.size(), is(2));
        assertThat(getVersionSource(nodes.get(1)).toString(), is("<http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip>"));
    }

    @Test
    public void handleDatasets() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        IRI refNode = toIRI("https://example.org/testing");
        registryReader = new RegistryReaderBioCASE(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("biocase-datasets.xml");
            }
        }, TestUtil.getTestCrawlContext(), nodes::add);

        registryReader.on(toStatement(toIRI("http://something/pywrapper.cgi?dsa="), HAS_VERSION, refNode));

        assertThat(nodes.size(), is(2));
        assertThat(getVersionSource(nodes.get(1)).getIRIString(), is("http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip"));
    }

    @Test
    public void parseDatasetsWithDCWA() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        // example from https://wiki.bgbm.org/bps/index.php/Archiving
        InputStream datasets = getClass().getResourceAsStream("biocase-datasets-example.xml");

        RegistryReaderBioCASE.parseDatasetInventory(datasets, nodes::add);

        assertThat(nodes.size(), is(4));
        assertThat(nodes.get(0).getObject().toString(), is("\"application/abcda\""));
        assertThat(getVersionSource(nodes.get(1)).toString(), is("<http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.ABCD_2.06.zip>"));
        assertThat(nodes.get(2).getObject().toString(), is("\"application/dwca\""));
        assertThat(getVersionSource(nodes.get(3)).toString(), is("<http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.DwCA.zip>"));
    }


}