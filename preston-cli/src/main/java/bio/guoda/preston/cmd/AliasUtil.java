package bio.guoda.preston.cmd;

import bio.guoda.preston.process.StatementsListener;
import org.apache.commons.rdf.api.Quad;

import java.util.function.Predicate;

public final class AliasUtil {

    public static void findSelectedAlias(StatementsListener listener, Predicate<Quad> selector, Persisting persisting) {
        ReplayUtil.replay(new SelectiveListener(selector, listener), persisting);
    }
}
