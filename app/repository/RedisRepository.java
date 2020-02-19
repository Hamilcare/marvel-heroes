package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import io.lettuce.core.protocol.RedisCommand;
import models.StatItem;
import models.TopStatItem;
import play.Logger;
import utils.StatItemSamples;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

@Singleton
public class RedisRepository {

    private static Logger.ALogger logger = Logger.of("RedisRepository");


    private final RedisClient redisClient;

    @Inject
    public RedisRepository(RedisClient redisClient) {
        this.redisClient = redisClient;
    }


    public CompletionStage<Boolean> addNewHeroVisited(StatItem statItem) {
        logger.info("hero visited " + statItem.name);
        return addHeroAsLastVisited(statItem).thenCombine(incrHeroInTops(statItem), (aLong, aBoolean) -> {
            return aBoolean && aLong > 0;
        });
    }

    private CompletionStage<Boolean> incrHeroInTops(StatItem statItem) {
        // TODO
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection
                .async()
                .zaddincr("TOP-HEROES", 1, statItem.toJson().toString())
                .thenApply(resp -> {
                    connection.close();
                    return true;
                });
//        return CompletableFuture.completedFuture(true);
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        System.out.println(statItem);
        StatefulRedisConnection<String, String> src = redisClient.connect();

        src.sync().lrem("LAST-HEROES",-1,statItem.toJson().toString());

        return src.async()
                .lpush("LAST-HEROES",  statItem.toJson().toString())
                .thenApply(resp -> {
                    src.close();
                    return resp;
                });


    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String, String> src = redisClient.connect();

        return src.async()
                .lrange("LAST-HEROES", 0, count-1)
                .thenApply(resp -> {
                    src.close();
                    List<StatItem> lastHeroes = new ArrayList<>(count);
                    resp.forEach(item -> lastHeroes.add(StatItem.fromJson(item)));
                    return lastHeroes;
                });

    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        StatefulRedisConnection<String, String> connection = redisClient.connect();

        return connection
                .async()
                .zrevrangeWithScores("TOP-HEROES", 0, 5)
                .thenApply(resp -> {
                    List<TopStatItem> topStatItems = new ArrayList<>(count);
                   resp.forEach(h -> topStatItems.add(new TopStatItem(StatItem.fromJson(h.getValue()), (long)h.getScore())));
                   return topStatItems;
                });
    }
}
