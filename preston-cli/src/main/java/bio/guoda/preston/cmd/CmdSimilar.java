package bio.guoda.preston.cmd;

import bio.guoda.preston.process.SimilarContentFinder;
import bio.guoda.preston.process.StatementsListener;
import bio.guoda.preston.store.BlobStore;
import bio.guoda.preston.store.HexaStore;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.beust.jcommander.converters.FloatConverter;
import com.beust.jcommander.converters.IntegerConverter;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.UUID;
import java.util.stream.Stream;

@Parameters(separators = "= ", commandDescription = "Describes similarity between contents identified by hash://sha256 IRIs according to their hash://tika-tlsh IRIs")
public class CmdSimilar extends CmdAppend {

    @Parameter(names = {"--hits", "--max-hits", "--matches", "--max-matches"}, description = "maximum number of similar contents to record for each content", converter = IntegerConverter.class)
    private int maxHits = 100;

    @Parameter(names = {"--thresh", "--threshold", "--similarity-threshold"}, description = "minimum similarity score required for a content similarity to be recorded", converter = FloatConverter.class)
    private float similarityThreshold = 100f;

    private String indexPath = Paths.get(getLocalTmpDir(), UUID.randomUUID().toString()).toString();
    private SimilarContentFinder similarContentFinder;

    @Override
    protected void run(BlobStore blobStore, HexaStore logRelations) {
        super.run(blobStore, logRelations);

        try {
            similarContentFinder.close();
        } catch (IOException e) {
            throw new RuntimeException("Failed to close search index.", e);
        } finally {
            FileUtils.deleteQuietly(new File(indexPath));
        }
    }

    public String getIndexPath() {
        return indexPath;
    }

    @Override
    protected Stream<StatementsListener> createProcessors(BlobStore blobStore, StatementsListener queueAsListener) {
        similarContentFinder = new SimilarContentFinder(blobStore, queueAsListener, getDataDir(indexPath), maxHits, similarityThreshold);
        return Stream.of(
                similarContentFinder
        );
    }

    @Override
    String getActivityDescription() {
        return "An event that generates similarity values between encountered contents (e.g., hash://sha256/... identifiers) that have been associated with a similarity hash (e.g., hash://tika-tlsh/...). The process currently uses similarity hashes (e.g., Trend Micro Locality Sensitive Hash, https://github.com/trendmicro/tlsh/blob/master/TLSH_CTC_final.pdf) calculated on content text extracted using https://tika.apache.org.";
    }

}
