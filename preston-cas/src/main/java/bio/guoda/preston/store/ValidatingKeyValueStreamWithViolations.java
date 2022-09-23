package bio.guoda.preston.store;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class ValidatingKeyValueStreamWithViolations implements ValidatingKeyValueStream {

    protected final List<String> violations = new ArrayList<>();

    @Override
    public List<String> getViolations() {
        return Collections.unmodifiableList(violations);
    }
}
