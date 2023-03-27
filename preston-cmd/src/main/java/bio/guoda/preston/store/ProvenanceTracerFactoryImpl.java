package bio.guoda.preston.store;

import bio.guoda.preston.process.ProcessorState;
import org.apache.commons.rdf.api.IRI;

import java.util.List;

public class ProvenanceTracerFactoryImpl implements ProvenanceTracerFactory {

    private final KeyValueStoreReadOnly blobstore;
    private final ProcessorState cmd;
    private final HexaStoreReadOnly hexastore;

    public ProvenanceTracerFactoryImpl(HexaStoreReadOnly hexastore,
                                       KeyValueStoreReadOnly blobstore,
                                       ProcessorState cmd) {
        this.blobstore = blobstore;
        this.cmd = cmd;
        this.hexastore = hexastore;
    }

    @Override
    public ProvenanceTracer create(List<IRI> provenanceAnchors) {
        ProvenanceTracer tracer = new ProvenanceTracerImpl(blobstore, cmd);
        if (provenanceAnchors == null || provenanceAnchors.isEmpty()) {
            tracer = new ProvenanceTracerByIndex(hexastore, tracer);
        }
        return tracer;
    }
}
