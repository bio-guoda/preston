package bio.guoda.preston.store;

import bio.guoda.preston.Hasher;
import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.junit.Assert.assertThat;

public class ValidatingKeyValueStreamSHA256IRITest {

    @Test
    public void validLength() throws IOException {
        IRI iri = Hasher.calcSHA256("bla");
        InputStream is = IOUtils.toInputStream(iri.getIRIString(), StandardCharsets.UTF_8);
        IRI somekey = RefNodeFactory.toIRI("somekey");
        ValidatingKeyValueStreamSHA256IRI someiri = new ValidatingKeyValueStreamSHA256IRI(is);
        IOUtils.copy(someiri.getValueStream(), new NullOutputStream());

        assertThat(someiri.acceptValueStreamForKey(somekey), Is.is(true));
    }

    @Test
    public void tooShort() throws IOException {
        InputStream is = IOUtils.toInputStream("short", StandardCharsets.UTF_8);
        IRI somekey = RefNodeFactory.toIRI("somekey");
        ValidatingKeyValueStreamSHA256IRI someiri = new ValidatingKeyValueStreamSHA256IRI(is);
        IOUtils.copy(someiri.getValueStream(), new NullOutputStream());

        assertThat(someiri.acceptValueStreamForKey(somekey), Is.is(false));
    }

    @Test
    public void tooLong() throws IOException {
        IRI iri = Hasher.calcSHA256("bla");
        InputStream is = IOUtils.toInputStream(iri.getIRIString() + "toolong", StandardCharsets.UTF_8);
        IRI somekey = RefNodeFactory.toIRI("somekey");
        ValidatingKeyValueStreamSHA256IRI someiri = new ValidatingKeyValueStreamSHA256IRI(is);
        IOUtils.copy(someiri.getValueStream(), new NullOutputStream());

        assertThat(someiri.acceptValueStreamForKey(somekey), Is.is(false));
    }

}