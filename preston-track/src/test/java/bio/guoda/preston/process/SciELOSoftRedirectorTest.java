package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class SciELOSoftRedirectorTest {

    @Test
    public void onSoftRedirect() {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("scielo-redirect.html");
            }
        };
        ArrayList<Quad> nodes = new ArrayList<>();
        SciELOSoftRedirector registryReader = new SciELOSoftRedirector(
                blobStore,
                TestUtilForProcessor.testListener(nodes)
        );
        Quad redirectResource = toStatement(
                toIRI("https://www.scielo.org.ar/scielo.php?script=sci_pdf&pid=S1667-782X2007000100006"),
                HAS_VERSION,
                createTestNode()
        );

        registryReader.on(redirectResource);

        assertThat(nodes.size(), is(3));
        Quad seeAlso = nodes.get(1);
        assertThat(seeAlso.getPredicate(), is(RefNodeConstants.ALTERNATE_OF));
        assertThat(seeAlso.getObject().toString(), is("<https://www.scielo.org.ar/pdf/ecoaus/v17n1/v17n1a06.pdf>"));

        Quad redirectedTo = nodes.get(2);
        assertThat(redirectedTo.getSubject().toString(), is("<https://www.scielo.org.ar/pdf/ecoaus/v17n1/v17n1a06.pdf>"));
        assertThat(redirectedTo.getPredicate(), is(HAS_VERSION));
        assertThat(RefNodeFactory.isBlankOrSkolemizedBlank(redirectedTo.getObject()), is(true));
    }
    @Test
    public void onPDF() {
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("scielo.pdf");
            }
        };
        ArrayList<Quad> nodes = new ArrayList<>();
        SciELOSoftRedirector registryReader = new SciELOSoftRedirector(
                blobStore,
                TestUtilForProcessor.testListener(nodes)
        );
        Quad pdfResourceVersionStatement = toStatement(
                toIRI("https://www.scielo.org.ar/scielo.php?script=sci_pdf&pid=S1667-782X2007000100006"),
                HAS_VERSION,
                createTestNode()
        );

        registryReader.on(pdfResourceVersionStatement);
        nodes.forEach(System.out::println);

        assertThat(nodes.size(), is(0));
    }

    private IRI createTestNode() {
        try {
            return toIRI(getClass().getResource("scielo-redirect.html").toURI());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(e);
        }
    }


}