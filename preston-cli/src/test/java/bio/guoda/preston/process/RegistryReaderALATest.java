package bio.guoda.preston.process;

import bio.guoda.preston.Seeds;
import bio.guoda.preston.model.RefNodeFactory;
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

import static bio.guoda.preston.MimeTypes.MIME_TYPE_JSON;
import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeConstants.WAS_ASSOCIATED_WITH;
import static bio.guoda.preston.TripleMatcher.hasTriple;
import static bio.guoda.preston.model.RefNodeFactory.getVersionSource;
import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static bio.guoda.preston.model.RefNodeFactory.toLiteral;
import static bio.guoda.preston.model.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.StringStartsWith.startsWith;

public class RegistryReaderALATest {

    @Test
    public void onSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderALA registryReader = new RegistryReaderALA(TestUtil.getTestBlobStore(), TestUtilForProcessor.testListener(nodes));
        registryReader.on(toStatement(Seeds.ALA, WAS_ASSOCIATED_WITH, toIRI("http://example.org/someActivity")));
        assertThat(nodes.size(), is(7));
        assertThat(getVersionSource(nodes.get(6)).getIRIString(), is("https://collections.ala.org.au/ws/dataResource?status=dataAvailable"));
    }

    @Test
    public void onEmptyPage() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderALA registryReader = new RegistryReaderALA(TestUtil.getTestBlobStore(), TestUtilForProcessor.testListener(nodes));

        registryReader.on(toStatement(toIRI("https://collections.ala.org.au/ws/dataResource?status=dataAvailable&resourceType=records"),
                HAS_VERSION,
                toIRI("https://some")));
        assertThat(nodes.size(), is(1));
    }

    @Test
    public void onNotSeed() {
        ArrayList<Quad> nodes = new ArrayList<>();
        RegistryReaderALA registryReader = new RegistryReaderALA(TestUtil.getTestBlobStore(), TestUtilForProcessor.testListener(nodes));
        RDFTerm bla = toLiteral("bla");
        registryReader.on(toStatement(Seeds.GBIF, toIRI("http://example.org"), bla));
        assertThat(nodes.size(), is(0));
    }

    @Test
    public void onRegistry() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("ala-dataresource-reg.json");
            }
        };
        RegistryReaderALA registryReader = new RegistryReaderALA(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://collections.ala.org.au/ws/dataResource?status=dataAvailable"), HAS_VERSION, createTestNode());

        registryReader.on(firstPage);

        assertThat(nodes.size(), is(3982));
        Quad secondPage = nodes.get(nodes.size() - 3);
        assertThat(secondPage, hasTriple(toStatement(toIRI("https://collections.ala.org.au/ws/dataResource/dr8052"), toIRI("http://purl.org/pav/createdBy"), toIRI("https://ala.org.au"))));
        secondPage = nodes.get(nodes.size() - 2);
        assertThat(secondPage, hasTriple(toStatement(toIRI("https://collections.ala.org.au/ws/dataResource/dr8052"), toIRI("http://purl.org/dc/elements/1.1/format"), RefNodeFactory.toLiteral(MIME_TYPE_JSON))));
        Quad thirdPage = nodes.get(nodes.size() - 1);
        assertThat(getVersionSource(thirdPage).toString(), is("<https://collections.ala.org.au/ws/dataResource/dr8052>"));
    }

    @Test
    public void onSingle() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("ala-dataresource.json");
            }
        };
        RegistryReaderALA registryReader = new RegistryReaderALA(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://collections.ala.org.au/ws/dataResource/dr6504"), HAS_VERSION, createTestNode());

        registryReader.on(firstPage);

        assertThat(nodes.size(), is(3));
        Quad first = nodes.get(1);
        assertThat(getVersionSource(first).toString(), is("<http://biocache.ala.org.au/archives/dr6504/dr6504_ror_dwca.zip>"));
        Quad second = nodes.get(2);
        assertThat(getVersionSource(second).toString(), is("<https://biocache.ala.org.au/archives/dr6504/dr6504_ror_dwca.zip>"));
    }

    @Test
    public void onSingleDwCA() {
        ArrayList<Quad> nodes = new ArrayList<>();
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("ala-dataresource-dwca.json");
            }
        };
        RegistryReaderALA registryReader = new RegistryReaderALA(blobStore, TestUtilForProcessor.testListener(nodes));


        Quad firstPage = toStatement(toIRI("https://collections.ala.org.au/ws/dataResource/dr6504"), HAS_VERSION, createTestNode());

        registryReader.on(firstPage);

        assertThat(nodes.size(), is(3));
        Quad first = nodes.get(1);
        assertThat(first.toString(), startsWith("<https://biocache.ala.org.au/archives/gbif/dr382/dr382.zip> <http://purl.org/dc/elements/1.1/format> \"application/dwca\" "));
        Quad second = nodes.get(2);
        assertThat(getVersionSource(second).toString(), startsWith("<https://biocache.ala.org.au/archives/gbif/dr382/dr382.zip>"));
    }

    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource("ala-dataresource-reg.json").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}