package bio.guoda.preston.store;

import bio.guoda.preston.DerefProgressListener;
import bio.guoda.preston.DerefState;
import org.apache.commons.rdf.api.IRI;
import org.junit.Test;

import java.net.URI;

public class KeyValueStoreUtilTest {

    @Test
    public void derefHttp() {
        KeyValueStoreUtil.getDerefStream(URI.create("https://example.org"), new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        }, null);
    }

    @Test
    public void derefFile() {
        KeyValueStoreUtil.getDerefStream(URI.create("file:///some/path"), new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        }, null);
    }

    @Test(expected = RuntimeException.class)
    public void derefNotSupported() {
        KeyValueStoreUtil.getDerefStream(URI.create("foo:///some/path"), new DerefProgressListener() {
            @Override
            public void onProgress(IRI dataURI, DerefState derefState, long read, long total) {

            }
        }, null);
    }

}