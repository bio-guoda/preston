package bio.guoda.preston.cmd;

import bio.guoda.preston.RDFUtil;
import bio.guoda.preston.RefNodeConstants;
import bio.guoda.preston.RefNodeFactory;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Quad;
import picocli.CommandLine;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;

@CommandLine.Command(
        name = "cat",
        aliases = {"get"},
        description = "Get biodiversity data"
)
public class CmdGet extends Persisting implements Runnable {

    @CommandLine.Parameters(description = "Content ids or known aliases (e.g., [hash://sha256/8ed311...])")
    private List<IRI> contentIdsOrAliases = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(
                getKeyValueStore(new ValidatingKeyValueStreamContentAddressedFactory()),
                true,
                getHashType()
        );
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        try {
            run(blobStore, contentIdsOrAliases);
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            throw new RuntimeException(th);
        }
    }

    protected void run(BlobStoreReadOnly blobStore, List<IRI> contentIdsOrAliases) throws IOException {
        if (contentIdsOrAliases.isEmpty()) {
            BufferedReader reader = new BufferedReader(new InputStreamReader(getInputStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println("attempting to read line as quad [" + line + "]");
                Quad quad = RDFUtil.asQuad(line);
                if (quad == null) {
                    System.out.println("attempting to read line as IRI [" + line + "]");
                    IRI queryIRI = toIRI(StringUtils.trim(line));
                    quad = RefNodeFactory.toStatement(RefNodeFactory.toBlank(), RefNodeConstants.HAS_VERSION, queryIRI);
                }
                ContentQueryUtil.copyMostRecentContent(blobStore, quad, this);
            }
        } else {
            for (IRI contentIdOrAlias : contentIdsOrAliases) {
                ContentQueryUtil.copyContent(blobStore, contentIdOrAlias, this);
            }
        }
    }

    public List<IRI> getContentIdsOrAliases() {
        return contentIdsOrAliases;
    }

    public void setContentIdsOrAliases(List<IRI> contentIdsOrAliases) {
        this.contentIdsOrAliases = contentIdsOrAliases;
    }


}
