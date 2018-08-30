package org.globalbioticinteractions.preston.process;

import org.globalbioticinteractions.preston.model.RefStatement;

public class StatementLogger extends StatementHashLogger {

    @Override
    protected String printStatement(RefStatement statement) {
        return statement.getSource().getLabel() + "\t" + statement.getRelationType().getLabel() + "\t" + statement.getTarget().getLabel();
    }


}
