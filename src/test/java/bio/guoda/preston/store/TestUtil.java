package bio.guoda.preston.store;

import org.apache.commons.io.IOUtils;
import org.apache.commons.rdf.api.IRI;
import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.process.BlobStoreReadOnly;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;

public class TestUtil {
    public static String toUTF8(InputStream content) throws IOException {
        return content == null ? null : IOUtils.toString(content, StandardCharsets.UTF_8);
    }

    public static KeyValueStore getTestPersistence() {
        return BlobStoreAppendOnlyTest.getTestPersistence();
    }

    public static BlobStoreReadOnly getTestBlobStore() {
        return new BlobStoreAppendOnly(getTestPersistence());
    }

    public static ActivityContext getTestCrawlContext() {
        return new ActivityContext() {
            @Override
            public IRI getActivity() {
                return toIRI("https://example.com/testActivity");
            }

        };
    }
}
