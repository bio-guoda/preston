package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefNode;
import org.globalbioticinteractions.preston.model.RefNodeFactory;
import org.globalbioticinteractions.preston.model.RefStatement;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class StatementLoggerTest {

    @Test
    public void relation() {
        RefNode source = RefNodeFactory.toLiteral("source");
        RefNode relation = RefNodeFactory.toLiteral("relation");
        RefNode target = RefNodeFactory.toLiteral("target");

        String str = new StatementLogger().printStatement(RefNodeFactory.toStatement(source, relation, target));

        assertThat(str, is("source\trelation\ttarget"));
    }


}