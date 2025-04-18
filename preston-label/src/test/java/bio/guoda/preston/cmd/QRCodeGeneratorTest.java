package bio.guoda.preston.cmd;

import bio.guoda.preston.RefNodeFactory;
import com.google.zxing.WriterException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.rdf.api.IRI;
import org.hamcrest.core.Is;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;

public class QRCodeGeneratorTest {

    @Rule
    public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Test
    public void encodeHash() throws WriterException, IOException {
        IRI iri = RefNodeFactory.toIRI("hash://sha256/5891b5b522d5df086d0ff0b110fbd9d21bb4fc7163af34d08286a2e846f6be03");
        ByteArrayOutputStream output = new ByteArrayOutputStream();


        QRCodeGenerator.generateQRCode(iri, output);
        String imageBytes = Base64.encodeBase64String(output.toByteArray());

        assertThat(imageBytes, Is.is("iVBORw0KGgoAAAANSUhEUgAAAQAAAAEACAIAAADTED8xAAAFI0lEQVR42u3bQXYbMQxEQd//0skZ/Ew0mpz6WzmKh2RpAVM//6QP92MJBIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASAdD2An1S/+jUOPtHcW829euMOAgAAAAAAAAAAAAAAAAAAXAgg9s6/Wuu/7NPcps49wtZnR+ydAQAAAAAAAAAAAAAAAAAAKgEcPHkHRxlzu3jwAQ8e4oOvzu0gAAAAAAAAAAAAAAAAAAAAADNToAf22OIAAIDFAQAAiwMAABYHAAAszoenQHMDlrnnPXi7oeSJAAAAAAAAAAAAAAAAAAAAvg0gRmvuQGydvAeOKQAAAAAAAAAAAAAAAAAAwOsASuYeXu2fXAEAAAAAAAAAAAAAAAAAAFwI4Ma2DlPnFOjNLXbKAQBAAAAgAAAQAAC0T4FK3nnrbM0NwUrGawAAAAAAAAAAAAAAAAAAAB8DMDfoiJ28uY+Dubfq3G4AAAAAAAAAAAAAAAAAAIALAcytV8kRj9192Lo3ETMMAAAAAAAAAAAAAAAAAADw3BQo5iE2MprbxYNn+uD/e+mJBwAAAAAAAAAAAAAAAAAA6AMQGzjMbdvWVfuSSY4pEAAAAAAAAAAAAAAAAADgKkRkVrN1FyA2b5k7xFtjvU9MgQAAAAAAAAAAAAAAAACAT/8leG5TY2u9NeiIjapiAL44BgUAAAAAAAAAAAAAAAAAvvWX4LkF2rp0UDIF2jrxMUsAAAAAAAAAAAAAAAAAAAD3A9gasMxdwdi6Hz/n3/cBAAAAAAAAAAAAAAAAAAAANjzE/m3snefOVmygNDd9AgAAAAAAAAAAAAAAAAAAuB9AbFix1dzp6fzhmGEAAAAAAAAAAAAAAAAAAID7ARxcr63pxNxQaOudY8cUAAAAAAAAAAAAAAAAAADgOQBz1+VjF/Fj9+O3Pjtiz/vFKRAAAAAAAAAAAAAAAAAA8GkAB0c3sWlMyadDyfRpa9oGAAAAAAAAAAAAAAAAAABwA4DYoGPrfMSEd65k5yULAAAAAAAAAAAAAAAAAACADgCxCwtbg46tH+78GOp8QAAAAAAAAAAAAAAAAAAAgA4AMUsHf8mfsbYOse8DAAAAAAAAAAAAAAAAAAAAzOzEwbFAbHEfOKaxJ/J9AAAAAAAAAAAAAAAAAAAAgI0TcHDbYjfgD/5wbOlKPqQAAAAAAAAAAAAAAAAAAABumALNTSeumD5tHdPYjGhrfwEAAAAAAAAAAAAAAAAAACoBxG6ib02BYutc8pWGnrsPAAAAAAAAAAAAAAAAAAAAT7c199g6aiWTq545DwAAAAAAAAAAAAAAAAAAQMEUKHYCOvc49sNz3yWIDdAAAAAAAAAAAAAAAAAAAADuB1Dyzgc3JnbySn7nLaUAAAAAAAAAAAAAAAAAAAD3A5i7ah87Lp2DjpIvANRelAAAAAAAAAAAAAAAAAAAAHgawByPuelT7Kg9edkBAAAAAAAAAAAAAAAAAADAFKj7hsJfhiSxtZoboAEAAAAAAAAAAAAAAAAAALwOIEYr9urcFOjgqZ37dCg5GwAAAAAAAAAAAAAAAAAAAB0Atv74PzeNeW8odMVqAAAAAAAAAAAAAAAAAAAAVAKQCgNAAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgASABIAEgD/QcMsZpBJZOrNAAAAABJRU5ErkJggg=="));
    }

}