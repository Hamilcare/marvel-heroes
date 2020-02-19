package repository;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.mongodb.client.model.*;
import com.mongodb.reactivestreams.client.MongoCollection;
import com.mongodb.reactivestreams.client.MongoDatabase;
import models.Hero;
import models.ItemCount;
import models.YearAndUniverseStat;
import org.bson.Document;
import org.bson.conversions.Bson;
import play.libs.Json;
import utils.HeroSamples;
import utils.*;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

@Singleton
public class MongoDBRepository {

    private final MongoCollection<Document> heroesCollection;

    @Inject
    public MongoDBRepository(MongoDatabase mongoDatabase) {
        this.heroesCollection = mongoDatabase.getCollection("heroes");
    }

    public CompletionStage<Optional<Hero>> heroById(String heroId) {
        Bson filter = Filters.eq("id", heroId);

        return ReactiveStreamsUtils.fromSinglePublisher(heroesCollection.find(filter).first())
                 .thenApply(result -> Optional.ofNullable(result).map(Document::toJson).map(Hero::fromJson));
    }

    public CompletionStage<List<YearAndUniverseStat>> countByYearAndUniverse() {
        HashMap groupFields1 = new HashMap() {{
            put("yearAppearance", "$identity.yearAppearance");
            put("universe", "$identity.universe");
        }};

        HashMap groupFields2 = new HashMap() {{
            put("yearAppearance", "$_id.yearAppearance");
        }};

        HashMap groupFields3 = new HashMap() {{
            put("universe", "$_id.universe");
            put("count", "$count");
        }};

        List<Bson> aggregates = Arrays.asList(
                Aggregates.match(Filters.ne("identity.yearAppearance", null)),
                Aggregates.group(groupFields1, Accumulators.sum("count", 1)),
                Aggregates.group(groupFields2,
                    Accumulators.addToSet("byUniverse", groupFields3)
                )
        );

        //Requête "façon MongoDB CLI"

//        List<Document> pipeline = new ArrayList<>();
//        pipeline.add(Document.parse("{\n" +
//                "        $match: { \"identity.yearAppearance\": { $ne: null } }\n" +
//                "    }"));
//        pipeline.add(Document.parse("{\n" +
//                "        $group: {\n" +
//                "            _id: {\n" +
//                "                yearAppearance: \"$identity.yearAppearance\",\n" +
//                "                universe: \"$identity.universe\"\n" +
//                "            },\n" +
//                "            count: { $sum: 1 }\n" +
//                "        }\n" +
//                "    }"));
//        pipeline.add(Document.parse("{\n" +
//                "        $group: {\n" +
//                "            _id: {\n" +
//                "                yearAppearance: \"$_id.yearAppearance\"\n" +
//                "            },\n" +
//                "            byUniverse: {\n" +
//                "                $push: {\n" +
//                "                    universe: \"$_id.universe\",\n" +
//                "                    count: \"$count\"\n" +
//                "                }\n" +
//                "            }\n" +
//                "        }\n" +
//                "    }"));


        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(aggregates))
                .thenApply(documents -> {
                    return documents.stream()
                                    .map(Document::toJson)
                                    .map(Json::parse)
                                    .map(jsonNode -> {
                                        int year = jsonNode.findPath("_id").findPath("yearAppearance").asInt();
                                        ArrayNode byUniverseNode = (ArrayNode) jsonNode.findPath("byUniverse");
                                        Iterator<JsonNode> elements = byUniverseNode.elements();
                                        Iterable<JsonNode> iterable = () -> elements;
                                        List<ItemCount> byUniverse = StreamSupport.stream(iterable.spliterator(), false)
                                                .map(node -> new ItemCount(node.findPath("universe").asText(), node.findPath("count").asInt()))
                                                .collect(Collectors.toList());
                                        return new YearAndUniverseStat(year, byUniverse);

                                    })
                                    .collect(Collectors.toList());
                });
    }


    public CompletionStage<List<ItemCount>> topPowers(int top) {
//        return CompletableFuture.completedFuture(new ArrayList<>());
        System.out.println("COUCOU");
        List<Bson> aggregates = Arrays.asList(
            Aggregates.unwind("$powers"),
            Aggregates.group("$powers", Accumulators.sum("count", 1)),
            Aggregates.sort(Sorts.descending("count")),
            Aggregates.limit(5)
        );

         return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(aggregates))
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
        List<Bson> aggregates = Arrays.asList(
                Aggregates.group("$identity.universe", Accumulators.sum("count", 1))
        );

        return ReactiveStreamsUtils.fromMultiPublisher(heroesCollection.aggregate(aggregates))
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
