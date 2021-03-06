package bio.guoda.preston.cmd;

import bio.guoda.preston.store.BlobStoreReadOnly;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.ContentHashDereferencer;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

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

    @Parameter(description = "data content-hash uri (e.g., [hash://sha256/8ed311...])",
            validateWith = URIValidator.class)
    private List<String> contentUris = new ArrayList<>();

    @Override
    public void run() {
        BlobStoreReadOnly blobStore = new BlobStoreAppendOnly(getKeyValueStore(new KeyValueStoreLocalFileSystem.ValidatingKeyValueStreamContentAddressedFactory()));
        run(blobStore);
    }

    public void run(BlobStoreReadOnly blobStore) {
        try {
            if (contentUris.isEmpty()) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
                String line;
                while ((line = reader.readLine()) != null) {
                    handleContentQuery(blobStore, StringUtils.trim(line));
                }
            } else {
                for (String s : contentUris) {
                    handleContentQuery(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }
    }

    protected void handleContentQuery(BlobStoreReadOnly blobStore, String queryString) throws IOException {
        ContentHashDereferencer query = new ContentHashDereferencer(blobStore);

        try {
            InputStream contentStream = query.dereference(toIRI(queryString));
            IOUtils.copyLarge(contentStream, System.out);
        } catch (IOException e) {
            throw new IOException("problem retrieving [" + queryString + "]", e);
        }
    }

    public void setContentUris(List<String> contentUris) {
        this.contentUris = contentUris;
    }

}
