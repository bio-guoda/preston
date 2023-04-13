package bio.guoda.preston.process;

import bio.guoda.preston.cmd.CopyShopNQuadToTSVTest;
import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.Quad;
import org.hamcrest.CoreMatchers;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.MatcherAssert.assertThat;

public class EmittingStreamOfAnyQuadTest {

    @Test
    public void withRelativeIRIs() {

        AtomicReference<Quad> foundQuad = new AtomicReference<>();

        ParsingEmitter emitting = new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                foundQuad.set(statement);
            }
        });

        emitting.parseAndEmit(IOUtils.toInputStream(CopyShopNQuadToTSVTest.exampleWithIRIObject(), StandardCharsets.UTF_8));


        Quad actual = foundQuad.get();
        assertThat(actual, Is.is(CoreMatchers.not(CoreMatchers.nullValue())));

        assertThat(actual.getGraphName().get().toString(), Is.is("<x-preston:d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae>"));
    }

    @Test
    public void withLiteralObjectWithRelativeIRIs() {

        AtomicReference<Quad> foundQuad = new AtomicReference<>();

        ParsingEmitter emitting = new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                foundQuad.set(statement);
            }
        });

        emitting.parseAndEmit(IOUtils.toInputStream(
                CopyShopNQuadToTSVTest.getLiteralObjectWithTypeAndRelativeGraphLabelIRI(), StandardCharsets.UTF_8)
        );


        Quad actual = foundQuad.get();
        assertThat(actual, Is.is(CoreMatchers.not(CoreMatchers.nullValue())));

        assertThat(actual.getObject().toString(), Is.is("\"2020-09-12T05:57:53.420Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime>"));
        assertThat(actual.getGraphName().get().toString(), Is.is("<x-preston:d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae>"));
    }

}