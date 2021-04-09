package bio.guoda.preston.process;

import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.stream.ContentStreamUtil;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;

import java.io.Closeable;

import static bio.guoda.preston.RefNodeConstants.HAS_VALUE;

/**
 * Shared Sketch Update Methods and Interfaces.
 */

abstract public class SketchBuilder extends StatementProcessor implements Closeable {

    public SketchBuilder(StatementsListener listener) {
        super(listener);
    }


    @Override
    public void on(Quad statement) {
        if (HAS_VALUE.equals(statement.getPredicate())
                && statement.getSubject() instanceof IRI) {

            IRI contentId = ContentStreamUtil.extractContentHash((IRI) statement.getSubject());

            updateSketch(statement, contentId);
        }
    }

    abstract protected void updateSketch(Quad statement, IRI contentId);

}
