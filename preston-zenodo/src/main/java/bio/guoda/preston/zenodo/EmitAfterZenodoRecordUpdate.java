package bio.guoda.preston.zenodo;

import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.cmd.EmitSelector;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.Quad;

import java.util.List;

class EmitAfterZenodoRecordUpdate implements EmitSelector {
    @Override
    public boolean shouldEmit(List<Quad> nodes) {
        long count = nodes.stream()
                .filter(q -> StringUtils.equals(RefNodeConstants.LAST_REFRESHED_ON.getIRIString(), q.getPredicate().getIRIString()))
                .count();
        return count > 0 || nodes.size() > 256;
    }
}
