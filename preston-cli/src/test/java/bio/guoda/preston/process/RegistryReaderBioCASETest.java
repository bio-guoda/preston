package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;

import static bio.guoda.preston.RefNodeConstants.HAD_MEMBER;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class RegistryReaderBioCASETest {

    private ArrayList<Quad> nodes;
    private StatementsListener registryReader;

    @Before
    public void init() {
        nodes = new ArrayList<>();
        registryReader = new RegistryReaderBioCASE(TestUtil.getTestBlobStore(), TestUtilForProcessor.testListener(nodes));
    }

    @Test
    public void onSeed() {
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.BIOCASE, WAS_ASSOCIATED_WITH, bla));
        assertThat(nodes.size(), is(5));
        assertThat(((IRI) getVersionSource(nodes.get(4))).getIRIString(), is(RegistryReaderBioCASE.BIOCASE_REGISTRY_ENDPOINT));
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

        IRI someHash = toIRI("hash://sha256/123");
        RegistryReaderBioCASE.parseProviders(providers, TestUtilForProcessor.testEmitter(nodes), someHash);

        assertFalse(nodes.isEmpty());
        assertThat(nodes.get(0).getPredicate(), is(HAD_MEMBER));
        assertThat(nodes.get(0).getObject().toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));
        assertThat(nodes.get(0).getSubject(), is(someHash));
        assertThat(getVersionSource(nodes.get(2)).toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));
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
        registryReader = new RegistryReaderBioCASE(blobStore, TestUtilForProcessor.testListener(nodes));
        registryReader.on(toStatement(toIRI(RegistryReaderBioCASE.BIOCASE_REGISTRY_ENDPOINT), HAS_VERSION, refNode));

        assertFalse(nodes.isEmpty());
        assertThat(nodes.get(1).getSubject(), is(refNode));
        assertThat(nodes.get(1).getPredicate(), is(RefNodeConstants.HAD_MEMBER));
        assertThat(nodes.get(1).getObject().toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));

        assertThat(nodes.get(2).getObject().toString(), is("\"text/xml\""));
        assertThat(nodes.get(2).getPredicate(), is(RefNodeConstants.HAS_FORMAT));
        assertThat(nodes.get(2).getSubject().toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));

        assertThat(nodes.get(3).getPredicate(), is(HAS_VERSION));
        assertThat(nodes.get(3).getSubject().toString(), is("<http://ww3.bgbm.org/biocase/pywrapper.cgi?dsa=GFBio_ColiFauna&inventory=1>"));

    }


    @Test
    public void parseDatasets() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        InputStream datasets = getClass().getResourceAsStream("biocase-datasets.xml");

        RegistryReaderBioCASE.parseDatasetInventory(datasets, TestUtilForProcessor.testEmitter(nodes), RefNodeFactory.toIRI("http://example.org"));

        assertThat(nodes.size(), is(3));
        assertThat(nodes.get(0).toString(), is("<http://example.org> <http://www.w3.org/ns/prov#hadMember> <http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip> ."));
        assertThat(nodes.get(1).getSubject().toString(), is("<http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip>"));
        assertThat(nodes.get(1).getPredicate().toString(), is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(nodes.get(1).getObject().toString(), is("\"application/abcda\""));
        assertThat(getVersionSource(nodes.get(2)).toString(), is("<http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip>"));
    }

    @Test
    public void handleDatasets() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        IRI someHash = toIRI("hash://sha256/1234");
        registryReader = new RegistryReaderBioCASE(new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("biocase-datasets.xml");
            }
        }, TestUtilForProcessor.testListener(nodes));

        registryReader.on(toStatement(toIRI("http://something/pywrapper.cgi?dsa="), HAS_VERSION, someHash));

        assertThat(nodes.size(), is(4));
        assertThat(nodes.get(1).getPredicate(), is(RefNodeConstants.HAD_MEMBER));
        assertThat(nodes.get(1).getSubject(), is(someHash));
        String zipArchive = "<http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip>";
        assertThat(nodes.get(1).getObject().toString(), is(zipArchive));

        assertThat(nodes.get(2).getPredicate(), is(RefNodeConstants.HAS_FORMAT));
        assertThat(nodes.get(2).getSubject().toString(), is(zipArchive));
        assertThat(nodes.get(2).getObject().toString(), is("\"application/abcda\""));

        assertThat(getVersionSource(nodes.get(3)).getIRIString(), is("http://ww3.bgbm.org/biocase/downloads/GFBio_ColiFauna/Coleoptera%20observations%20in%20orchards%20of%20South%20Western%20Germany.ABCD_2.06.zip"));
    }

    @Test
    public void parseDatasetsWithDCWA() throws IOException, XPathExpressionException, SAXException, ParserConfigurationException {
        // example from https://wiki.bgbm.org/bps/index.php/Archiving
        InputStream datasets = getClass().getResourceAsStream("biocase-datasets-example.xml");

        RegistryReaderBioCASE.parseDatasetInventory(datasets, TestUtilForProcessor.testEmitter(nodes), RefNodeFactory.toIRI("http://example.org"));

        assertThat(nodes.size(), is(6));
        assertThat(nodes.get(0).toString(), is("<http://example.org> <http://www.w3.org/ns/prov#hadMember> <http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.ABCD_2.06.zip> ."));
        assertThat(nodes.get(1).getObject().toString(), is("\"application/abcda\""));
        assertThat(getVersionSource(nodes.get(2)).toString(), is("<http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.ABCD_2.06.zip>"));
        assertThat(nodes.get(3).toString(), is("<http://example.org> <http://www.w3.org/ns/prov#hadMember> <http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.DwCA.zip> ."));
        assertThat(nodes.get(4).getObject().toString(), is("\"application/dwca\""));
        assertThat(getVersionSource(nodes.get(5)).toString(), is("<http://ww3.bgbm.org/biocase/downloads/Herbar/Herbarium%20Berolinense.DwCA.zip>"));
    }


}