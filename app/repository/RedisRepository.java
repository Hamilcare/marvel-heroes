package repository;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.api.StatefulRedisConnection;
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
        return CompletableFuture.completedFuture(true);
    }


    private CompletionStage<Long> addHeroAsLastVisited(StatItem statItem) {
        // TODO
//        return CompletableFuture.completedFuture(1L);
        System.out.println(statItem);
        StatefulRedisConnection<String,String>src = redisClient.connect();
        long result = src.sync().zadd("LAST",0.0, statItem.toJson());
        System.out.println("ADD OK MAGGLE");
        src.close();
        return CompletableFuture.completedFuture(result);

    }

    public CompletionStage<List<StatItem>> lastHeroesVisited(int count) {
        logger.info("Retrieved last heroes");
        StatefulRedisConnection<String,String>src = redisClient.connect();
        List<StatItem> result = src.sync().zrange("LAST", 0, count).stream().map(string -> StatItem.fromJson(string)).collect(Collectors.toList());
        src.close();
        return CompletableFuture.completedFuture(result);
    }

    public CompletionStage<List<TopStatItem>> topHeroesVisited(int count) {
        logger.info("Retrieved tops heroes");
        // TODO
        List<TopStatItem> tops = Arrays.asList(new TopStatItem(StatItemSamples.MsMarvel(), 8L), new TopStatItem(StatItemSamples.Starlord(), 6L), new TopStatItem(StatItemSamples.SpiderMan(), 5L), new TopStatItem(StatItemSamples.BlackPanther(), 5L), new TopStatItem(StatItemSamples.Thanos(), 4L));
        return CompletableFuture.completedFuture(tops);
    }
}
