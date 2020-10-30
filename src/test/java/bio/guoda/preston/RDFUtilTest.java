package bio.guoda.preston;

import bio.guoda.preston.model.RefNodeFactory;
import bio.guoda.preston.process.EmittingStreamRDF;
import bio.guoda.preston.process.StatementProcessor;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.BlankNodeOrIRI;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import org.apache.commons.rdf.api.RDFTerm;
import org.apache.jena.riot.RiotException;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class RDFUtilTest {


    @Test
    public void asQuadStream() {
        Stream<Quad> quadStream = RDFUtil.asQuadStream(IOUtils.toInputStream("<subj> <verb> <obj> <graph> .", StandardCharsets.UTF_8));
        List<Quad> collect = quadStream.collect(Collectors.toList());
        assertThat(collect.size(), is(1));
        Quad quad = collect.get(0);
        assertThat(quad.getObject().toString(), is("<obj>"));
        assertThat(quad.getPredicate().toString(), is("<verb>"));
        assertThat(quad.getSubject().toString(), is("<subj>"));
        assertThat(quad.getGraphName().isPresent(), is(true));
        assertThat(quad.getGraphName().get().toString(), is("<graph>"));
    }

    @Test
    public void valueForDate() {
        final String valueFor = RDFUtil.getValueFor(RefNodeFactory
                .toLiteral("2020-09-12T05:54:48.034Z", RefNodeFactory.toIRI("http://www.w3.org/2001/XMLSchema#dateTime")));

        assertThat(valueFor, is("2020-09-12T05:54:48.034Z"));
    }

    @Test(expected = RiotException.class)
    public void invalidRDF() {
        String nquad = "this ain't no RDF";

        EmittingStreamRDF rdfStream = new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
            }
        });

        rdfStream.parseAndEmit(IOUtils.toInputStream(nquad, StandardCharsets.UTF_8));
    }

}