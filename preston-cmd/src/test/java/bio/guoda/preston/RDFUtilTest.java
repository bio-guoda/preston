package bio.guoda.preston;

import bio.guoda.preston.process.EmittingStreamOfAnyVersions;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.Quad;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicBoolean;

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

        EmittingStreamOfAnyVersions rdfStream = new EmittingStreamOfAnyVersions(new StatementsEmitterAdapter() {
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

        EmittingStreamOfAnyVersions rdfStream = new EmittingStreamOfAnyVersions(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                statementFound.set(true);
            }
        });

        rdfStream.parseAndEmit(getClass().getResourceAsStream("prov.nq"));

        assertTrue(statementFound.get());
    }

}