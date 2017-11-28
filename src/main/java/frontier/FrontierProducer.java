package frontier;

import crawler.TwitterCrawler;
import domain.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;

import java.util.Optional;

@Component
public class FrontierProducer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontierProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CrawledUserRepository crawledUserRepository;
    @Autowired
    private CrawledFollowingRepository crawledFollowingRepository;
    @Autowired
    private CrawledTweetsRepository crawledTweetsRepository;

    private static final String FRONTIER_FOLLOWING = "followingfrontier";
    private static final String FRONTIER_TWEETS = "tweetsfrontier";
    private static final String FRONTIER_USER = "userfrontier";

    public void sendFollowing(String twitterIdStr) {
        send(FRONTIER_FOLLOWING, twitterIdStr, TwitterCrawler.CRAWLED_DATA_TYPE.FOLLOWING,
                crawledFollowingRepository);
    }

    public void sendTweets(String twitterIdStr) {
        send(FRONTIER_TWEETS, twitterIdStr, TwitterCrawler.CRAWLED_DATA_TYPE.TWEETS,
                crawledTweetsRepository);
    }

    public void sendUser(String twitterIdStr) {
        send(FRONTIER_USER, twitterIdStr, TwitterCrawler.CRAWLED_DATA_TYPE.USER,
                crawledUserRepository);
    }

    public void send(String topic,
                     String twitterIdStr,
                     TwitterCrawler.CRAWLED_DATA_TYPE crawledDataType,
                     CrudRepository crudRepository){

        LOGGER.info("try to send twitter id='{}' to topic='{}'", twitterIdStr, topic);

        long twitterId = Long.parseLong(twitterIdStr);

        CrawledData crawledData = (CrawledData) Optional
                .ofNullable(crudRepository.findOne(twitterId))
                .orElse(CrawledDataFactory.getInstance(crawledDataType, twitterId));

        if (TwitterCrawler.isToCrawl(crawledData)){
            crawledData.setCrawlStatus(0);
            crudRepository.save(crawledData);
            kafkaTemplate.send(topic, ""+twitterId);
        }
    }
}