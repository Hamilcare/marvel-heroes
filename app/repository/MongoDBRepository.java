package repository;

import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import play.libs.Json;
import utils.HeroSamples;
import utils.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }


    public CompletionStage<Optional<Hero>> heroById(String heroId) {
//        return HeroSamples.staticHero(heroId);
         String query = "{id: \"" + heroId + "\"}";
         Document document = Document.parse(query);
         return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(document).first())
                 .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        return CompletableFuture.completedFuture(new ArrayList<>());
        // TODO
        //List<Document> pipeline = new ArrayList<>();
        //return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
        //        .thenApply(documents -> {
        //            return documents.stream()
        //                            .map(Document::toJson)
        //                            .map(Json::parse)
        //                            .map(jsonNode -> {
        //                                int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
        //                                ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
        //                                Iterator<JsonNode> elements = byUniverseNode.elements();
        //                                Iterable<JsonNode> iterable = () -> elements;
        //                                List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
        //                                        .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
        //                                        .collect(Collectors.toList());
        //                                return new YearAndUniverseStat(year, byUniverse);
        //
        //                            })
        //                            .collect(Collectors.toList());
        //        });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
//        return CompletableFuture.completedFuture(new ArrayList<>());

         List<Document> pipeline = new ArrayList<>();
         pipeline.add(Document.parse("{\n" +
                 "        $unwind: {\n" +
                 "            path: \"$powers\"\n" +
                 "        }\n" +
                 "    }"));
        pipeline.add(Document.parse("{\n" +
                "        $group: {\n" +
                "            _id: \"$powers\",\n" +
                "            count: { $sum: 1 }\n" +
                "        }\n" +
                "    }"));
        pipeline.add(Document.parse("{\n" +
                "        $sort: {\n" +
                "            count: -1\n" +
                "        }\n" +
                "    }"));
        pipeline.add(Document.parse("{\n" +
                "        $limit: 5\n" +
                "    }"));
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

    public CompletionStage<List<ItemCount>> byUniverse() {
//        return CompletableFuture.completedFuture(new ArrayList<>());
         List<Document> pipeline = new ArrayList<>();
         String query = "{\n" +
                 "        $group: {\n" +
                 "            _id: \"$identity.universe\",\n" +
                 "            count: { $sum: 1}\n" +
                 "        }\n" +
                 "    }";
         pipeline.add(Document.parse(query));
         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(pipeline))
                 .thenApply(documents -> {
                     return documents.stream()
                             .map(Document::toJson)
                             .map(Json::parse)
                             .map(jsonNode -> {
                                 return new ItemCount(jsonNode.findPath("_id").asText(), jsonNode.findPath("count").asInt());
                             })
                             .collect(Collectors.toList());
                 });
    }

}
