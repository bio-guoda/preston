package bio.guoda.preston.stream;

import org.apache.commons.rdf.api.IRI;

public class ArchiveEntryStreamHandler extends ArchiveStreamHandler {
    private final IRI targetIri;

    public ArchiveEntryStreamHandler(ContentStreamFactory contentStreamFactory, IRI targetIri) {
        super(contentStreamFactory);
        this.targetIri = targetIri;
    }

    @Override
    protected boolean shouldReadArchiveEntry(IRI entryIri) {
        return isPartOfTargetIri(entryIri);
    }

    private boolean isPartOfTargetIri(IRI iri) {
        return targetIri.getIRIString().contains(iri.getIRIString());
    }

}
