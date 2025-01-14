package bio.guoda.preston.cmd;

import org.apache.commons.rdf.api.IRI;

public class VerificationEntry {
    private final IRI iri;
    private VerificationState state;
    private IRI calculatedHashIRI;
    private Long fileSize;

    public VerificationEntry(IRI expectedHash, VerificationState state, IRI calculatedHashIRI, Long fileSize) {
        this.iri = expectedHash;
        this.state = state;
        this.calculatedHashIRI = calculatedHashIRI;
        this.fileSize = fileSize;
    }

    public VerificationState getState() {
        return state;
    }

    public IRI getCalculatedHashIRI() {
        return calculatedHashIRI;
    }

    public Long getFileSize() {
        return fileSize;
    }

    public IRI getIri() {
        return iri;
    }

    public void setState(VerificationState state) {
        this.state = state;
    }

    public void setFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public void setCalculatedHashIRI(IRI calculatedHashIRI) {
        this.calculatedHashIRI = calculatedHashIRI;
    }
}
