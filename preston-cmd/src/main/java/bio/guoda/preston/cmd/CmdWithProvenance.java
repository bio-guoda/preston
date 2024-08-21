package bio.guoda.preston.cmd;

import org.apache.commons.rdf.api.IRI;
import picocli.CommandLine;

import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH;
import static bio.guoda.preston.RefNodeConstants.BIODIVERSITY_DATASET_GRAPH_UUID_STRING;

public class CmdWithProvenance extends Cmd {

    public static final IRI PROVENANCE_ANCHOR_DEFAULT = BIODIVERSITY_DATASET_GRAPH;
    @CommandLine.Option(
            names = {"--anchor", "-r", "--provenance-root", "--provenance-anchor"},
            defaultValue = "urn:uuid:" + BIODIVERSITY_DATASET_GRAPH_UUID_STRING,
            description = "specify the provenance root/anchor of the command. By default, any available data graph will be traversed up to it's most recent additions. If the provenance root is set, only specified provenance signature and their origins are included in the scope."
    )
    private IRI provenanceAnchor = PROVENANCE_ANCHOR_DEFAULT;

    public IRI getProvenanceAnchor() {
        return this.provenanceAnchor;
    }

    public void setProvenanceArchor(IRI provenanceAnchor) {
        this.provenanceAnchor = provenanceAnchor;
    }


}
