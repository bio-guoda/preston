package bio.guoda.preston.store;

import bio.guoda.preston.cmd.ActivityContext;
import bio.guoda.preston.process.StatementsEmitter;
import bio.guoda.preston.process.StatementsEmitterAdapter;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.process.StatementsListenerAdapter;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;

public class TestUtilForProcessor {
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
