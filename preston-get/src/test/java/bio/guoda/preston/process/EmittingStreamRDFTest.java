package bio.guoda.preston.process;

import bio.guoda.preston.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.MatcherAssert.assertThat;

public class EmittingStreamRDFTest {

    @Test
    public void emitQuad() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .", StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(quads.get(0).getObject().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#SoftwareAgent>"));

    }

    @Test
    public void emitWithBlankNode() {
        List<Quad> quads = new ArrayList<>();

        String nquad = "<https://example.org> <http://purl.org/pav/hasVersion> _:ae63fa95-362c-38b5-b74f-203f8d7f92b3 .";

        new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream(nquad, StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(RefNodeFactory.isBlankOrSkolemizedBlank(quads.get(0).getObject()), Is.is(true));

    }

    @Test
    public void emitQuadURIWithUUID() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<urn:uuid:5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .", StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(quads.get(0).getSubject().ntriplesString(), Is.is("<urn:uuid:5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1>"));

    }

    @Test
    public void emitQuadStopAfterFirst() {
        AtomicInteger counter = new AtomicInteger(0);
        List<Quad> quads = new ArrayList<>();
        String line0 = "<https://preston.guoda.org> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/prov#SoftwareAgent> .";
        new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }, new ProcessorState() {
            @Override
            public boolean shouldKeepProcessing() {
                return counter.getAndIncrement() == 0;
            }

            @Override
            public void stopProcessing() {

            }
        }).parseAndEmit(IOUtils.toInputStream(StringUtils.joinWith("\n", line0, line0), StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(quads.get(0).getObject().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#SoftwareAgent>"));

    }

    @Test
    public void emitQuadNeverStart() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamRDF(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }, new ProcessorState() {
            @Override
            public boolean shouldKeepProcessing() {
                return false;
            }

            @Override
            public void stopProcessing() {

            }
        }).parseAndEmit(IOUtils.toInputStream("bla bla", StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(0));

    }

}