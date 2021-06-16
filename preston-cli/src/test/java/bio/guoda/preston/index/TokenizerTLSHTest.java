package bio.guoda.preston.index;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.QueryBuilder;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class TokenizerTLSHTest {

    @Test
    public void tokenize() {
        QueryBuilder queryBuilder = new QueryBuilder(getAnalyzer());
        Query query = queryBuilder.createBooleanQuery("", "deadbeef");

        assertThat(query.toString(), is("3 5 11 14 18 22 27 29 34 39 43 46 51 54 59 63"));
    }

    public static Analyzer getAnalyzer() {
        return new Analyzer() {
            @Override
            protected Analyzer.TokenStreamComponents createComponents(String fieldName) {
                Tokenizer source = new TokenizerTLSH();
                return new Analyzer.TokenStreamComponents(source);
            }
        };
    }
}
