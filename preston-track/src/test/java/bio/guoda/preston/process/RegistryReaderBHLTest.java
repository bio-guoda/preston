package bio.guoda.preston.process;

import bio.guoda.preston.HashType;
import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtil;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toLiteral;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class RegistryReaderBHLTest {

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        registryReader.on(toStatement(Seeds.BHL, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        assertThat(nodes.size(), is(6));
        assertThat(getVersionSource(nodes.get(5)).getIRIString(), is("https://www.biodiversitylibrary.org/data/item.txt"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));

        registryReader.on(toStatement(toIRI("https://api.gbif.org/v1/dataset"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(TestUtil.getTestBlobStore(HashType.sha256), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.BHL, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onItems() {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("bhl_item.txt");
            }
        };
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://www.biodiversitylibrary.org/data/item.txt"), HAS_VERSION, createTestNode());

        registryReader.on(firstPage);

        assertThat(nodes.size(), is(64));
        Quad derivedFrom = nodes.get(1);
        assertThat(derivedFrom.getSubject().toString(), is("<mobot31753000022803>"));
        assertThat(derivedFrom.getPredicate().toString(), is("<http://www.w3.org/ns/prov#wasDerivedFrom>"));
        assertThat(derivedFrom.getObject().toString(), endsWith("item.txt!/L2>"));
        assertThat(derivedFrom.getObject().toString(), startsWith("<line:"));


        Quad mimeType = nodes.get(nodes.size() - 2);
        assertThat(mimeType.getSubject().toString(), is("<https://archive.org/download/mobot31753002306964/mobot31753002306964_djvu.txt>"));
        assertThat(mimeType.getPredicate().toString(), is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(mimeType.getObject().toString(), is("\"text/plain;charset=UTF-8\""));
        Quad secondPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<https://archive.org/download/mobot31753002306964/mobot31753002306964_djvu.txt>"));

        mimeType = nodes.get(nodes.size() - 5);
        assertThat(mimeType.getSubject().toString(), is("<https://archive.org/download/mobot31753002306964/mobot31753002306964_meta.xml>"));
        assertThat(mimeType.getPredicate().toString(), is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(mimeType.getObject().toString(), is("\"text/xml\""));
        secondPage = nodes.get(nodes.size() - 4);
        assertThat(getVersionSource(secondPage).toString(), is("<https://archive.org/download/mobot31753002306964/mobot31753002306964_meta.xml>"));
    }


    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource("bhl_item.txt").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}