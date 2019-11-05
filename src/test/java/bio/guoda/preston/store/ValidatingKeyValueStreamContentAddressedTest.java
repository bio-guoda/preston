package bio.guoda.preston.store;

import bio.guoda.preston.model.RefNodeFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.NullOutputStream;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;

public class ValidatingKeyValueStreamContentAddressedTest {

    @Test
    public void matchingHash() throws IOException {
        InputStream is = IOUtils.toInputStream("some value", StandardCharsets.UTF_8);
        ValidatingKeyValueStreamContentAddressed validating = new ValidatingKeyValueStreamContentAddressed(is);
        IOUtils.copy(validating.getValueStream(), new NullOutputStream());
        assertFalse(validating.acceptValueStreamForKey(RefNodeFactory.toIRI("bla")));
        assertTrue(validating.acceptValueStreamForKey(RefNodeFactory.toIRI("hash://sha256/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")));
    }

    @Test
    public void mismatchingHash() throws IOException {
        InputStream is = IOUtils.toInputStream("some other value", StandardCharsets.UTF_8);
        ValidatingKeyValueStreamContentAddressed validating = new ValidatingKeyValueStreamContentAddressed(is);
        IOUtils.copy(validating.getValueStream(), new NullOutputStream());
        assertFalse(validating.acceptValueStreamForKey(RefNodeFactory.toIRI("hash://sha256/e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")));
    }

}