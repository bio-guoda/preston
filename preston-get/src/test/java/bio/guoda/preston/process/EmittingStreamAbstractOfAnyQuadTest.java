package bio.guoda.preston.process;

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

public class EmittingStreamAbstractOfAnyQuadTest {

    @Test
    public void emitQuad() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<https://bisque.cyverse.org/image_service/image/00-fXkw8KPeArtBjNrpPHtvTU/resize:1250/format:jpeg> <http://purl.org/pav/hasVersion> <hash://sha256/a8770ba1ae326327fddb91178ed58cc97462e78d6cdb97370a578f31a79ff817> <urn:uuid:85f490ff-f3dc-40a1-974d-a21eec3b6ca5> .", StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(quads.get(0).getSubject().ntriplesString(), Is.is("<https://bisque.cyverse.org/image_service/image/00-fXkw8KPeArtBjNrpPHtvTU/resize:1250/format:jpeg>"));
        assertThat(quads.get(0).getPredicate().ntriplesString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(quads.get(0).getObject().ntriplesString(), Is.is("<hash://sha256/a8770ba1ae326327fddb91178ed58cc97462e78d6cdb97370a578f31a79ff817>"));

    }

    @Test
    public void shouldNotEmitBlankNode() {
        List<Quad> quads = new ArrayList<>();

        String nquad = "<https://example.org> <http://purl.org/pav/hasVersion> _:ae63fa95-362c-38b5-b74f-203f8d7f92b3 .";

        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream(nquad, StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(0));
    }

    @Test
    public void emitQuadURIWithUUID() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<urn:uuid:5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1> <http://www.w3.org/ns/prov#usedBy> <http://www.w3.org/ns/prov#SoftwareAgent> .", StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(quads.get(0).getSubject().ntriplesString(), Is.is("<urn:uuid:5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1>"));
        assertThat(quads.get(0).getPredicate().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#usedBy>"));
        assertThat(quads.get(0).getObject().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#SoftwareAgent>"));
        assertThat(quads.get(0).getGraphName().isPresent(), Is.is(false));

    }

    @Test
    public void emitQuadURIWithUUIDAndGraphName() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }).parseAndEmit(IOUtils.toInputStream("<urn:uuid:5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1> <http://www.w3.org/ns/prov#usedBy> <http://www.w3.org/ns/prov#SoftwareAgent> <foo:bar> .", StandardCharsets.UTF_8));

        assertThat(quads.size(), Is.is(1));

        assertThat(quads.get(0).getSubject().ntriplesString(), Is.is("<urn:uuid:5b0c34bb-fa0a-4dbb-947a-ef93afcad8b1>"));
        assertThat(quads.get(0).getPredicate().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#usedBy>"));
        assertThat(quads.get(0).getObject().ntriplesString(), Is.is("<http://www.w3.org/ns/prov#SoftwareAgent>"));
        assertThat(quads.get(0).getGraphName().isPresent(), Is.is(true));
        assertThat(quads.get(0).getGraphName().get().ntriplesString(), Is.is("<foo:bar>"));

    }

    @Test
    public void emitQuadStopAfterFirst() {
        AtomicInteger counter = new AtomicInteger(0);
        List<Quad> quads = new ArrayList<>();
        String line0 = "<foo:bar> <http://www.w3.org/ns/prov#usedBy> <foo:bar> .";
        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
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

        assertThat(quads.get(0).getObject().ntriplesString(), Is.is("<foo:bar>"));

    }

    @Test
    public void emitQuadsSkipMalformed() {
        // for some reason line 114 of hash://sha256/18b51a180c63929d5e3a50dbb72295579c2645546d22ae3fdcd5e2095c43d199
        // contains malformed rdf quads
        // also see https://github.com/bio-guoda/preston/issues/214
        // E.g.,
        // preston cat --remote https://linker.bio 'line:hash://sha256/18b51a180c63929d5e3a50dbb72295579c2645546d22ae3fdcd5e2095c43d199!/L114'
        //
        // produces:
        //
        // 001/XMLSchema#dateTime> <urn:uuid:8ae298b0-bf2a-4ce4-aa44-b41b0a0d0f6a> .
        //
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                quads.add(statement);
            }
        }, new ProcessorState() {
            @Override
            public boolean shouldKeepProcessing() {
                return true;
            }

            @Override
            public void stopProcessing() {

            }
        }).parseAndEmit(getClass().getResourceAsStream("/bio/guoda/preston/store/issue-214-data-example/18/b5/18b51a180c63929d5e3a50dbb72295579c2645546d22ae3fdcd5e2095c43d199"));

        assertThat(quads.size(), Is.is(113));
    }

    @Test
    public void emitQuadNeverStart() {
        List<Quad> quads = new ArrayList<>();
        new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
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