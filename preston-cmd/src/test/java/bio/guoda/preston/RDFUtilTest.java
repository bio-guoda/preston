package bio.guoda.preston;

import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertNotNull;
import static junit.framework.TestCase.assertTrue;
import static junit.framework.TestCase.fail;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RDFUtilTest {


    @Test
    public void valueForDate() {
        final String valueFor = RDFUtil.getValueFor(RefNodeFactory
                .toLiteral("2020-09-12T05:54:48.034Z", RefNodeFactory.toIRI("http://www.w3.org/2001/XMLSchema#dateTime")));

        assertThat(valueFor, is("2020-09-12T05:54:48.034Z"));
    }

    @Test
    public void ignoreInvalidRDF() {
        String nquad = "this ain't no RDF";

        EmittingStreamRDF rdfStream = new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                fail("no statement is expected");
            }
        });

        rdfStream.parseAndEmit(IOUtils.toInputStream(nquad, StandardCharsets.UTF_8));
    }


    @Test
    public void parseRDF() {

        AtomicBoolean statementFound = new AtomicBoolean(false);

        EmittingStreamRDF rdfStream = new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statementFound.set(true);
            }
        });

        rdfStream.parseAndEmit(getClass().getResourceAsStream("prov.nq"));

        assertTrue(statementFound.get());
    }

    @Test
    public void parseAsQuad() {

        Quad quad = RDFUtil.asQuad("<https://preston.guoda.bio> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> <d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae> .");

        assertNotNull(quad);
        assertThat(quad.getSubject().toString(), is("https://preston.guoda.bio"));
    }

}