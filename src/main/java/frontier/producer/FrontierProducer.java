package frontier.producer;

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
        send(FRONTIER_FOLLOWING, twitterIdStr, CrawledDataFactory.CRAWLED_DATA_TYPE.FOLLOWING, crawledFollowingRepository);
    }

    public void sendTweets(String twitterIdStr) {
        send(FRONTIER_TWEETS, twitterIdStr, CrawledDataFactory.CRAWLED_DATA_TYPE.TWEETS, crawledTweetsRepository);
    }

    public void sendUser(String twitterIdStr) {
        send(FRONTIER_USER, twitterIdStr, CrawledDataFactory.CRAWLED_DATA_TYPE.USER, crawledUserRepository);
    }

    public void send(String topic,
                     String twitterIdStr,
                     CrawledDataFactory.CRAWLED_DATA_TYPE crawledDataType,
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

    public void sendFollowingTransaction(CrawledFollowing crawledFollowing) {
        LOGGER.info("sending tweets of user id='{}' to topic='followingtransactiona'", crawledFollowing.getTwitterID());

        String following = crawledFollowing.getDataCrawled();
        if (following == null || following.isEmpty()){
            crawledFollowing.setCrawlStatus(TwitterCrawler.NOTHING_TO_SYNC);
        } else{
            crawledFollowing.setCrawlStatus(TwitterCrawler.SYNC_INIT);
            String payload = crawledFollowing.getTwitterID()+","+crawledFollowing.getDataCrawled();
            kafkaTemplate.send("followingtransactiona", payload);
        }
        crawledFollowingRepository.save(crawledFollowing);
    }

    public void sendUserTransaction(CrawledUser crawledUser) {
        LOGGER.debug("sending user transaction message for twitter-id='{}', topic='usertransactiona'", crawledUser.getTwitterID());

        if (crawledUser.getDataCrawled()==null || crawledUser.getDataCrawled().isEmpty()){
            crawledUser.setCrawlStatus(TwitterCrawler.NOTHING_TO_SYNC);
        } else{
            crawledUser.setCrawlStatus(TwitterCrawler.SYNC_INIT);
            String payload = crawledUser.getTwitterID()+":"+crawledUser.getDataCrawled();
            kafkaTemplate.send("usertransactiona", payload);
        }
        crawledUserRepository.save(crawledUser);
    }

    public void sendTweetsTransaction(CrawledTweets crawledTweets) {
        LOGGER.info("sending tweets of user id='{}' to topic='tweetstransactiona'", crawledTweets.getTwitterID());

        String tweets = crawledTweets.getDataCrawled();
        if (tweets == null || tweets.isEmpty() || "[]".equals(crawledTweets.getDataCrawled())){
            crawledTweets.setCrawlStatus(TwitterCrawler.NOTHING_TO_SYNC);
        } else{
            crawledTweets.setCrawlStatus(TwitterCrawler.SYNC_INIT);
            String payload = crawledTweets.getTwitterID()+":"+crawledTweets.getDataCrawled();
            kafkaTemplate.send("tweetstransactiona", payload);
        }
        crawledTweetsRepository.save(crawledTweets);
    }
}