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
         return wsClient.url(elasticConfiguration.uri+"/heroes/_search")
                 .post(Json.parse(
                         "{" +

                         "  \"query\": {\n" +
                         "    \"multi_match\" : {\n" +
                         "      \"query\" : \"" + input + "\",\n" +
                         "      \"fields\" : [ \"name^4\", \"aliases^3\", \"secretIdentities^3\", \"description^2\", \"partners^1\"] \n" +
                         "    }\n" +
                         "  }" +
                         "}"
                 ))
                 .thenApply(response -> {
                    Iterator<JsonNode> it = response.asJson().get("hits").get("hits").iterator();
                    List<SearchedHero> myResearchedHeroes = new ArrayList<>();
                     while(it.hasNext()){
                         JsonNode next = it.next().get("_source");
                         System.out.println(next);
                        myResearchedHeroes.add(SearchedHero.fromJson(next));
                    }
                     return new PaginatedResults<SearchedHero>(5,1,1,myResearchedHeroes);

                 });


    }

    public CompletionStage<List<SearchedHero>> suggest(String input) {
        return CompletableFuture.completedFuture(Arrays.asList(SearchedHeroSamples.IronMan(), SearchedHeroSamples.MsMarvel(), SearchedHeroSamples.SpiderMan()));
        // TODO
        // return wsClient.url(elasticConfiguration.uri + "...")
        //         .post(Json.parse("{ ... }"))
        //         .thenApply(response -> {
        //             return ...
        //         });
    }
}
