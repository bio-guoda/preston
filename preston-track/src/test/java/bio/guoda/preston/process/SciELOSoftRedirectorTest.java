package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.TestUtilForProcessor;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.globalbioticinteractions.doi.DOI;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static bio.guoda.preston.RefNodeFactory.toIRI;
import static bio.guoda.preston.RefNodeFactory.toStatement;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.Is.is;

public class SciELOSoftRedirectorTest {

    @Test
    public void chileDOI() {
        String scieloUrl = "https://www.scielo.cl/scielo.php?script=sci_pdf&pid=S0717-65382015000100003";

        Pattern chilePattern = Pattern.compile("http[s]{0,1}://www.scielo.cl/.*pid=(?<pid>[A-Za-z0-9-]+)");
        String scieloDOI = "";
        Matcher matcher = chilePattern.matcher(scieloUrl);
        if (matcher.matches()) {
            scieloDOI = new DOI("4067", StringUtils.lowerCase(matcher.group("pid"))).toURI().toString();
        }
        assertThat(scieloDOI, is("https://doi.org/10.4067/s0717-65382015000100003"));
    }


    @Test
    public void brazilDOI() {
        String scieloUrl = "http://www.scielo.br/scielo.php?script=sci_arttext&pid=S2236-89062014000200010";
        String scieloDOI = "";
        Pattern brazilPattern = Pattern.compile("http[s]{0,1}://www.scielo.br/.*pid=(?<pid>[A-Za-z0-9-]+)");
        Matcher matcher = brazilPattern.matcher(scieloUrl);
        if (matcher.matches()) {
            scieloDOI = new DOI("1590", StringUtils.lowerCase(matcher.group("pid"))).toURI().toString();
        }
        assertThat(scieloDOI, is("https://doi.org/10.1590/s2236-89062014000200010"));
    }

    @Test
    public void argentinaDOI() {
        String scieloUrl = "http://www.scielo.org.ar/scielo.php?script=sci_arttext&pid=S0327-93832015000100002";
        String scieloDOI = null;
        assertThat(scieloDOI, is(nullValue()));
    }

    @Test
    public void mexicoDOI() {
        String scieloUrl = "http://www.scielo.org.mx/scielo.php?script=sci_arttext&pid=S0065-17372004000300015";
        String scieloDOI = null;
        assertThat(scieloDOI, is(nullValue()));
    }

    @Test
    public void colombiaDOI() {
        String scieloUrl = "http://www.scielo.org.co/scielo.php?script=sci_pdf&pid=S0123-30682014000200013";
        String scieloDOI = null;
        assertThat(scieloDOI, is(nullValue()));
    }

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
    public void onSoftRedirectChile() {
        // see https://github.com/bio-guoda/preston/issues/336#issuecomment-3005353035
        // no soft redirect present likely due to cloudflare bot wall
        BlobStoreReadOnly blobStore = new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) throws IOException {
                return getClass().getResourceAsStream("scielo-redirect-chile.html");
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

        assertThat(nodes.size(), is(0));
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