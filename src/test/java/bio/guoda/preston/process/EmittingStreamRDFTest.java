package bio.guoda.preston.process;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.cmd.ProcessorState;
import bio.guoda.preston.model.RefNodeFactory;
import com.sun.xml.internal.bind.api.impl.NameConverter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Triple;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static bio.guoda.preston.RefNodeConstants.HAS_VERSION;
import static org.junit.Assert.*;

public class EmittingStreamRDFTest {

    @Test
    public void emitQuad() {
        List<Triple> triples = new ArrayList<>();
        new EmittingStreamRDF(new StatementEmitter() {
            @Override
            public void emit(Triple statement) {
                triples.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .", StandardCharsets.UTF_8));

        assertThat(triples.size(), Is.is(1));

        assertThat(triples.get(0).getObject().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#SoftwareAgent>"));

    }

    @Test
    public void emitWithBlankNode() {
        List<Triple> triples = new ArrayList<>();

        String nquad = "<https://example.org> <http://purl.org/pav/hasVersion> _:ae63fa95-362c-38b5-b74f-203f8d7f92b3 .";

        new EmittingStreamRDF(new StatementEmitter() {
            @Override
            public void emit(Triple statement) {
                triples.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream(nquad, StandardCharsets.UTF_8));

        assertThat(triples.size(), Is.is(1));

        assertThat(RefNodeFactory.isBlankOrSkolemizedBlank(triples.get(0).getObject()), Is.is(true));

    }

    @Test
    public void emitQuadURIWithUUID() {
        List<Triple> triples = new ArrayList<>();
        new EmittingStreamRDF(new StatementEmitter() {
            @Override
            public void emit(Triple statement) {
                triples.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .", StandardCharsets.UTF_8));

        assertThat(triples.size(), Is.is(1));

        assertThat(triples.get(0).getSubject().ntriplesString(), Is.is("<5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1>"));

    }

    @Test
    public void emitQuadStopAfterFirst() {
        AtomicInteger counter = new AtomicInteger(0);
        List<Triple> triples = new ArrayList<>();
        String line0 = "<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .";
        new EmittingStreamRDF(new StatementEmitter() {
            @Override
            public void emit(Triple statement) {
                triples.add(statement);
            }
        }, new ProcessorState() {
            @Override
            public boolean shouldKeepProcessing() {
                return counter.getAndIncrement() == 0;
            }
        }).parseAndEmit(IOUtils.toInputStream(StringUtils.joinWith("\n", line0, line0), StandardCharsets.UTF_8));

        assertThat(triples.size(), Is.is(1));

        assertThat(triples.get(0).getObject().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#SoftwareAgent>"));

    }

    @Test
    public void emitQuadNeverStart() {
        List<Triple> triples = new ArrayList<>();
        new EmittingStreamRDF(new StatementEmitter() {
            @Override
            public void emit(Triple statement) {
                triples.add(statement);
            }
        }, new ProcessorState() {
            @Override
            public boolean shouldKeepProcessing() {
                return false;
            }
        }).parseAndEmit(IOUtils.toInputStream("bla bla", StandardCharsets.UTF_8));

        assertThat(triples.size(), Is.is(0));

    }

}