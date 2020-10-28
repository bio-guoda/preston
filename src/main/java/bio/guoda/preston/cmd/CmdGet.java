package bio.guoda.preston.cmd;

import bio.guoda.preston.process.BlobStoreReadOnly;
import bio.guoda.preston.store.BlobStoreAppendOnly;
import bio.guoda.preston.store.KeyValueStoreLocalFileSystem;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.rdf.api.IRI;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static bio.guoda.preston.model.RefNodeFactory.toIRI;
import static java.lang.System.exit;
import static org.apache.commons.io.IOUtils.EOF;

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
                    handleContentIri(blobStore, StringUtils.trim(line));
                }
            } else {
                for (String s : contentUris) {
                    handleContentIri(blobStore, s);
                }
            }
        } catch (Throwable th) {
            th.printStackTrace(System.err);
            exit(1);
        }
    }

    protected void handleContentIri(BlobStoreReadOnly blobStore, String queryString) throws IOException {
        ContentQuery query = new ContentQuery(queryString);
        IRI contentHash = query.getContentIri();

        try {
            InputStream input = blobStore.get(contentHash);
            if (input == null) {
                System.err.print("not found: [" + contentHash.getIRIString() + "]\n");
                exit(1);
            }
            processQuery(query, input);

        } catch (IOException e) {
            throw new IOException("problem retrieving [" + contentHash.getIRIString() + "]", e);
        }
    }

    private void processQuery(ContentQuery query, InputStream is) throws IOException {
        InputStream copyStream = query.dereference(is);
        copyIfNoError(copyStream, System.out);
    }

    protected void copyIfNoError(InputStream proxyIs, PrintStream out) throws IOException {
        byte[] buffer = new byte[4096];
        int n;
        while (this.shouldKeepProcessing() && !out.checkError() && EOF != (n = proxyIs.read(buffer))) {
            System.out.write(buffer, 0, n);
        }
    }

    public void setContentUris(List<String> contentUris) {
        this.contentUris = contentUris;
    }

    private class ContentQuery {
        final Pattern matchOuterQuery = Pattern.compile("^(?<operator>[a-zA-Z0-9]+):(?<inner>\\S+)!\\\\/(?<qualifiers>\\S*)$");
        LinkedList<Pair<String, String>> queryParts = new LinkedList<>();
        private final IRI contentIri;

        public ContentQuery(String query) {
            Matcher matcher = matchOuterQuery.matcher(query);

            while (matcher.find()) {
                queryParts.addFirst(Pair.of(matcher.group("operator"), matcher.group("qualifiers")));
            }

            if (queryParts.size() > 0) {
                contentIri = toIRI(matcher.group("inner"));
            }
            else {
                contentIri = toIRI(query);
            }
        }

        public IRI getContentIri() { return contentIri; }

        public Stream<Pair<String, String>> getQueryStream() {
            return queryParts.stream();
        }

        public InputStream dereference(InputStream is) {
            return is;
        }
    }

}
