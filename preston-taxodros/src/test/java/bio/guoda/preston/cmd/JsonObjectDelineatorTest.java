package bio.guoda.preston.cmd;

import bio.guoda.preston.store.TestUtil;
import org.apache.commons.lang3.tuple.Pair;
import org.hamcrest.core.Is;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.hamcrest.MatcherAssert.assertThat;

public class JsonObjectDelineatorTest {

    @Test
    public void streamSingleZoteroArticle() throws IOException {
        String resourceName = "ZoteroArticle.json";
        InputStream is = getResource(resourceName);

        List<Pair<Long, Long>> locations = new ArrayList<>();


        Consumer<Pair<Long, Long>> listener = new Consumer<Pair<Long, Long>>() {

            @Override
            public void accept(Pair<Long, Long> jsonLocationJsonLocationPair) {
                locations.add(jsonLocationJsonLocationPair);
            }
        };


        JsonObjectDelineator.locateTopLevelObjects(is, listener);


        assertThat(locations.size(), Is.is(1));
        assertThat(locations.get(0).getLeft(), Is.is(1L));
        assertThat(locations.get(0).getRight(), Is.is(4122L));

    }

    private InputStream getResource(String resourceName) throws IOException {
        return TestUtil.filterLineFeedFromTextInputStream(getClass().getResourceAsStream(resourceName));
    }

    @Test
    public void streamZoteroArticleToZenodoLineJson() throws IOException {
        InputStream resourceAsStream = getResource("ZoteroArticleList.json");

        List<Pair<Long, Long>> locations = new ArrayList<>();


        Consumer<Pair<Long, Long>> listener = new Consumer<Pair<Long, Long>>() {

            @Override
            public void accept(Pair<Long, Long> jsonLocationJsonLocationPair) {
                locations.add(jsonLocationJsonLocationPair);
            }
        };


        JsonObjectDelineator.locateTopLevelObjects(resourceAsStream, listener);


        assertThat(locations.size(), Is.is(1));
        assertThat(locations.get(0).getLeft(), Is.is(3L));
        assertThat(locations.get(0).getRight(), Is.is(4124L));

    }

    @Test
    public void streamZoteroArticles() throws IOException {
        InputStream resourceAsStream = getResource("ZoteroArticleListWithMany.json");

        List<Pair<Long, Long>> locations = new ArrayList<>();


        Consumer<Pair<Long, Long>> listener = new Consumer<Pair<Long, Long>>() {

            @Override
            public void accept(Pair<Long, Long> jsonLocationJsonLocationPair) {
                locations.add(jsonLocationJsonLocationPair);
            }
        };


        JsonObjectDelineator.locateTopLevelObjects(resourceAsStream, listener);


        assertThat(locations.size(), Is.is(100));
        assertThat(locations.get(0).getLeft(), Is.is(7L));
        assertThat(locations.get(0).getRight(), Is.is(5524L));
        assertThat(locations.get(99).getLeft(), Is.is(360141L));
        assertThat(locations.get(99).getRight(), Is.is(363965L));
    }

}
