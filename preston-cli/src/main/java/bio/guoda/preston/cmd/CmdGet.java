package bio.guoda.preston.cmd;

import bio.guoda.preston.IRIFixingProcessor;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static bio.guoda.preston.RefNodeFactory.toIRI;
import static java.lang.System.exit;

@Parameters(separators = "= ", commandDescription = "get biodiversity data")
public class CmdGet extends Persisting implements Runnable {

    @Parameter(description = "content ids or known aliases (e.g., [hash://sha256/8ed311...])",
            validateWith = URIValidator.class)
    private List<String> contentIdsOrAliases = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        try {
            if (contentIdsOrAliases.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    handleContentQuery(blobStore, StringUtils.trim(line), this);
                }
            } else {
                for (String s : contentIdsOrAliases) {
                    handleContentQuery(blobStore, s, this);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }
    }

    protected void handleContentQuery(BlobStoreReadOnly blobStore, String queryString, Persisting persisting) throws IOException {
        IRI queryIRI = toIRI(queryString);

        BlobStoreReadOnly query = resolvingBlobStore(blobStore);

        try {
            InputStream contentStream = query.get(
                    new IRIFixingProcessor()
                            .process(queryIRI)
            );
            IOUtils.copyLarge(contentStream, System.out);
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryString + "]", e);
        }
    }

    public void setContentIdsOrAliases(List<String> contentIdsOrAliases) {
        this.contentIdsOrAliases = contentIdsOrAliases;
    }

}
