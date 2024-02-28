package bio.guoda.preston.cmd;

import org.apache.commons.rdf.api.Quad;

import java.util.List;

public interface EmitSelector {

    boolean shouldEmit(List<Quad> nodes);

}
