package bio.guoda.preston.store;

import bio.guoda.preston.process.ProcessorState;
import bio.guoda.preston.process.StatementListener;
import org.apache.commons.rdf.api.IRI;

import java.io.IOException;

import static bio.guoda.preston.RefNodeFactory.toStatement;

public class TracerOfDescendants implements ProvenanceTracer {

    private final HexaStoreReadOnly hexastore;

    private final ProcessorState cmd;


    public TracerOfDescendants(HexaStoreReadOnly hexastore, ProcessorState cmd) {
        this.hexastore = hexastore;
        this.cmd = cmd;

    }

    @Override
    public void trace(IRI provenanceAnchor, StatementListener listener) throws IOException {
        VersionUtil.findMostRecentVersion(provenanceAnchor, getHexastore(), listener);
    }


    public HexaStoreReadOnly getHexastore() {
        return hexastore;
    }

}
