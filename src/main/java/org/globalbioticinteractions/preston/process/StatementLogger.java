package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

public class StatementLogger extends StatementHashLogger {

    @Override
    protected String printStatement(RefStatement statement) {
        return statement.getSubject().getLabel() + "\t" + statement.getPredicate().getLabel() + "\t" + statement.getObject().getLabel();
    }


}
