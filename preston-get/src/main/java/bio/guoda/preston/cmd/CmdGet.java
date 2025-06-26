package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.ValidatingKeyValueStreamContentAddressedFactory;
import bio.guoda.preston.store.VersionUtil;
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
        description = "Get content",
        footer =
                Cmd.BUGS +
                "%n%nExample:%n%n" +
                "# get a picture of a bunny%n" +
                "preston cat\\%n" +
                " --remote https://wikimedia.org\\%n" +
                " --remote https://linker.bio\\%n" +
                " hash://sha1/86fa30f32d9c557ea5d2a768e9c3595d3abb17a2\\%n" +
                " > bunny.jpg"
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
                Quad quad = VersionUtil.parseAsVersionStatementOrNull(line);
                if (quad != null) {
                    ContentQueryUtil.copyMostRecentContent(blobStore, quad, this, new CopyShopImpl(this));
                }
            }
        } else {
            for (IRI contentIdOrAlias : contentIdsOrAliases) {
                ContentQueryUtil.copyContent(blobStore, contentIdOrAlias, this, new CopyShopImpl(this));
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
