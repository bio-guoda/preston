package bio.guoda.preston;

import org.apache.commons.rdf.api.Triple;
import bio.guoda.preston.cmd.Logger;
import bio.guoda.preston.process.StatementListener;
import bio.guoda.preston.process.StatementLoggerNQuads;
import bio.guoda.preston.process.StatementLoggerTSV;

import java.util.concurrent.atomic.AtomicLong;

public class StatementLogFactory {
    public static StatementListener createLogger(Logger logMode) {
        StatementListener logger;
        if (Logger.tsv == logMode) {
            logger = new StatementLoggerTSV();
        } else if (Logger.nquads == logMode) {
            logger = new StatementLoggerNQuads();
        } else {
            logger = new StatementListener() {
                AtomicLong count = new AtomicLong(1);

                @Override
                public void on(Triple statement) {
                    long index = count.getAndIncrement();
                    if ((index % 80) == 0) {
                        System.out.println();
                    } else {
                        System.out.print(".");
                    }
                }
            };
        }
        return logger;
    }
}
