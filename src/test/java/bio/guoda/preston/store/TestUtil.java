package bio.guoda.preston.store;

import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

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

    public static BlobStoreReadOnly getTestBlobStoreForResource(String pathToResource) {
        return new BlobStoreReadOnly() {
            @Override
            public InputStream get(IRI key) {
                return getClass().getResourceAsStream(pathToResource);
            }
        };
    }

    public static ActivityContext getTestCrawlContext() {
        return new ActivityContext() {
            @Override
            public IRI getActivity() {
                return toIRI("https://example.com/testActivity");
            }

            @Override
            public String getDescription() {
                return "this is a test activity";
            }

        };
    }

    public static InputStream filterLineFeedFromTextInputStream(InputStream is) throws IOException {
        // Git client for windows is configured to insert \r (carriage returns) when checking out text files ; See e.g., https://github.com/nodejs/node/pull/20754
        // this causes issues when using hashes that reply on byte sequences.
        // https://help.github.com/en/github/using-git/configuring-git-to-handle-line-endings
        String s = IOUtils.toString(is, StandardCharsets.UTF_8);
        String replace = StringUtils.replace(s, "\r\n", "\n");
        return IOUtils.toInputStream(replace, StandardCharsets.UTF_8);
    }

    public static StatementsListener testListener(List<Quad> nodes) {
        return new TestListener(nodes);
    }

    public static StatementsEmitter testEmitter(List<Quad> nodes) {
        return new TestEmitter(nodes);
    }

    public static class TestListener extends StatementsListenerAdapter {
        private final List<Quad> nodes;

        public TestListener(List<Quad> nodes) {
            this.nodes = nodes;
        }

        @Override
        public void on(Quad statement) {
            nodes.add(statement);
        }
    }
    public static class TestEmitter extends StatementsEmitterAdapter {
        private final List<Quad> nodes;

        public TestEmitter(List<Quad> nodes) {
            this.nodes = nodes;
        }

        @Override
        public void emit(Quad statement) {
            nodes.add(statement);
        }
    }
}
