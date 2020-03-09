package bio.guoda.preston.process;

import bio.guoda.preston.Seeds;
import bio.guoda.preston.store.TestUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.commons.rdf.api.Triple;
import org.apache.commons.rdf.api.Quad;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
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

public class RegistryReaderBHLTest {

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(TestUtil.getTestBlobStore(), nodes::add);
        registryReader.on(toStatement(Seeds.BHL, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        Assert.assertThat(nodes.size(), is(5));
        assertThat(getVersionSource(nodes.get(4)).getIRIString(), is("https://www.biodiversitylibrary.org/data/item.txt"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(TestUtil.getTestBlobStore(), nodes::add);

        registryReader.on(toStatement(toIRI("https://api.gbif.org/v1/dataset"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderBHL registryReader = new RegistryReaderBHL(TestUtil.getTestBlobStore(), nodes::add);
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
        RegistryReaderBHL registryReader = new RegistryReaderBHL(blobStore, nodes::add);


        Quad firstPage = toStatement(toIRI("https://www.biodiversitylibrary.org/data/item.txt"), HAS_VERSION, createTestNode());

        registryReader.on(firstPage);

        Assert.assertThat(nodes.size(), is(36));
        Quad mimeType = nodes.get(nodes.size() - 2);
        assertThat(mimeType.getSubject().toString(), is("<https://archive.org/download/mobot31753002306964/mobot31753002306964_djvu.txt>"));
        assertThat(mimeType.getPredicate().toString(), is("<http://purl.org/dc/elements/1.1/format>"));
        assertThat(mimeType.getObject().toString(), is("\"text/plain;charset=UTF-8\""));
        Quad secondPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(secondPage).toString(), is("<https://archive.org/download/mobot31753002306964/mobot31753002306964_djvu.txt>"));
    }


    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource("bhl_item.txt").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}