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

        assertThat(actual.getGraphName().get().toString(), Is.is("<x:preston:d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae>"));
    }

    @Test
    public void withStartTime() {

        AtomicReference<Quad> foundQuad = new AtomicReference<>();

        ParsingEmitter emitting = new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                foundQuad.set(statement);
            }
        });

        emitting.parseAndEmit(IOUtils.toInputStream("<urn:uuid:88948228-e967-415c-8cc6-b3f4b4c77a26> <http://www.w3.org/ns/prov#startedAtTime> \"2023-04-13T02:25:42.491Z\"^^<http://www.w3.org/2001/XMLSchema#dateTime> <urn:uuid:88948228-e967-415c-8cc6-b3f4b4c77a26> .", StandardCharsets.UTF_8));


        Quad actual = foundQuad.get();
        assertThat(actual, Is.is(CoreMatchers.not(CoreMatchers.nullValue())));

        assertThat(actual.getGraphName().get().toString(), Is.is("<urn:uuid:88948228-e967-415c-8cc6-b3f4b4c77a26>"));
        assertThat(actual.getObject().toString(), Is.is("<urn:uuid:88948228-e967-415c-8cc6-b3f4b4c77a26>"));
        assertThat(actual.getPredicate().toString(), Is.is("<urn:uuid:88948228-e967-415c-8cc6-b3f4b4c77a26>"));
        assertThat(actual.getSubject().toString(), Is.is("<urn:uuid:88948228-e967-415c-8cc6-b3f4b4c77a26>"));
    }

    @Test
    public void withVersionIRIs() {
        AtomicReference<Quad> foundQuad = new AtomicReference<>();

        ParsingEmitter emitting = new EmittingStreamOfAnyQuad(new StatementsEmitterAdapter() {
            @Override
            public void emit(Quad statement) {
                foundQuad.set(statement);
            }
        });

        emitting.parseAndEmit(IOUtils.toInputStream("<http://mczbase.mcz.harvard.edu/specimen_images/entomology/paleo/large/PALE-2014_Andrena_clavula_holotype.jpg> <http://purl.org/pav/hasVersion> <hash://sha256/71be33fe0c41f51c9f82fc5c195fa832d2c720a9e64bc7c9dd3ea725b194d96a> <b43b22f6-9298-4e49-a1a6-0eb9d4133931> .", StandardCharsets.UTF_8));

        Quad actual = foundQuad.get();
        assertThat(actual, Is.is(CoreMatchers.not(CoreMatchers.nullValue())));

        assertThat(actual.getGraphName().get().toString(), Is.is("<x:preston:b43b22f6-9298-4e49-a1a6-0eb9d4133931>"));
        assertThat(actual.getSubject().toString(), Is.is("<http://mczbase.mcz.harvard.edu/specimen_images/entomology/paleo/large/PALE-2014_Andrena_clavula_holotype.jpg>"));
        assertThat(actual.getPredicate().toString(), Is.is("<http://purl.org/pav/hasVersion>"));
        assertThat(actual.getObject().toString(), Is.is("<hash://sha256/71be33fe0c41f51c9f82fc5c195fa832d2c720a9e64bc7c9dd3ea725b194d96a>"));
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
        assertThat(actual.getGraphName().get().toString(), Is.is("<x:preston:d2c8a96a-89c8-4dd6-ba37-06809d4ff9ae>"));
    }

}