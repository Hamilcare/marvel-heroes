package repository;

import com.fasterxml.jackson.databind.JsonNode;
import env.ElasticConfiguration;
import env.MarvelHeroesConfiguration;
import models.PaginatedResults;
import models.SearchedHero;
import play.libs.Json;
import play.libs.ws.WSClient;
import utils.SearchedHeroSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

@Singleton
public class ElasticRepository {

    private final WSClient wsClient;
    private final ElasticConfiguration elasticConfiguration;

    @Inject
    public ElasticRepository(WSClient wsClient, MarvelHeroesConfiguration configuration) {
        this.wsClient = wsClient;
        this.elasticConfiguration = configuration.elasticConfiguration;
    }


    public CompletionStage<PaginatedResults<SearchedHero>> searchHeroes(String input, int size, int page) {
//        return CompletableFuture.completedFuture(new PaginatedResults<>(3, 1, 1, Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan())));
//         TODO
        System.out.println("size : " + size);
        System.out.println("page : " + page);
        String query;
        if (input.isEmpty()) {
            query = "{\"query\":{\"size\":1000}}";
        } else {
            query = "{\"query\":{\"query_string\":{\"query\":\"" + input + "\",\"fields\":[\"name^4\",\"aliases^3\",\"secretIdentities^3\",\"description^2\",\"partners^1\"]}}}";
        }
        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse(
                        query
                ))
                .thenApply(response -> {
                    Iterator<JsonNode> it = response.asJson().get("hits").get("hits").iterator();
                    List<SearchedHero> myResearchedHeroes = new ArrayList<>();
                    int total = Integer.parseInt(response.asJson().get("hits").get("total").get("value").toString());
                    while (it.hasNext()) {
                        JsonNode next = it.next().get("_source");
//                         System.out.println(next);
                        myResearchedHeroes.add(SearchedHero.fromJson(next));
                    }
                    return new PaginatedResults<SearchedHero>(total, page, total / size, myResearchedHeroes);

                });


    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
//        return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        // TODO

        return wsClient.url(elasticConfiguration.uri + "/heroes/_search")
                .post(Json.parse("{\"suggest\":{\"heroes-suggest\":{\"prefix\":\"" + input + "\",\"completion\":{\"field\":\"suggest\",\"size\":5}}}}"))
                .thenApply(response -> {

                    Iterator<JsonNode> it = response.asJson().get("suggest").get("heroes-suggest").get(0).get("options").iterator();
                    List<SearchedHero> myResearchedHeroes = new ArrayList<>();
                    while (it.hasNext()) {
                        JsonNode next = it.next().get("_source");

                        myResearchedHeroes.add(SearchedHero.fromJson(next));
                    }
                    return (myResearchedHeroes);
                });
    }
}
