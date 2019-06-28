package bio.guoda.preston;

import org.apache.commons.rdf.api.IRI;

public interface DerefProgressListener {
    void onProgress(IRI dataURI, DerefState derefState, long read, long total);
}
