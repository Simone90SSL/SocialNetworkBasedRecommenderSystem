package frontier;

import crawler.CrawlerContextConfiguration;
import crawler.TwitterCralwerFactory;
import crawler.TwitterCrawler;
import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.tweets.TweetsCrawlerContextConfiguration;
import crawler.user.UserCrawlerContextConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;
import synchronization.SynchronizationProducer;
import twitter4j.TwitterException;

@Component
public class FrontierConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(FrontierConsumer.class);

    @Autowired
    private UserCrawlerContextConfiguration userCrawlerContextConfiguration;
    @Autowired
    private FollowingCrawlerContextConfiguration followingCrawlerContextConfiguration;
    @Autowired
    private TweetsCrawlerContextConfiguration tweetsCrawlerContextConfiguration;

    @Autowired
    private CrawledUserRepository crawledUserRepository;
    @Autowired
    private CrawledFollowingRepository crawledFollowingRepository;
    @Autowired
    private CrawledTweetsRepository crawledTweetsRepository;

    @Autowired
    private FrontierProducer frontierProducer;
    @Autowired
    private SynchronizationProducer synchronizationProducer;

    @KafkaListener(topics = "${kafka.topic.followingfrontier}")
    public void receiveFollowing(String TwitterId) {
        receive(TwitterId,
                TwitterCrawler.CRAWLED_DATA_TYPE.FOLLOWING,
                followingCrawlerContextConfiguration,
                crawledFollowingRepository);
    }

    @KafkaListener(topics = "${kafka.topic.tweetsfrontier}")
    public void receiveTweets(String TwitterId) {
        receive(TwitterId,
                TwitterCrawler.CRAWLED_DATA_TYPE.TWEETS,
                tweetsCrawlerContextConfiguration,
                crawledTweetsRepository);
    }

    @KafkaListener(topics = "${kafka.topic.userfrontier}")
    public void receiveUser(String TwitterId) {
        receive(TwitterId,
                TwitterCrawler.CRAWLED_DATA_TYPE.USER,
                userCrawlerContextConfiguration,
                crawledUserRepository);
    }

    private void receive(String TwitterId,
                        TwitterCrawler.CRAWLED_DATA_TYPE crawledDataType,
                        CrawlerContextConfiguration crawlerContextConfiguration,
                        CrudRepository crudRepository){
        LOGGER.info("Get twitter id '{}' from the frontier '{}'", TwitterId, crawledDataType);
        TwitterCrawler twitterCrawler = null;
        try {
            twitterCrawler = TwitterCralwerFactory.getInstance(
                    crawledDataType,
                    crawlerContextConfiguration,
                    frontierProducer,
                    crudRepository);
        } catch (TwitterException te) {
            LOGGER.error("Twitter4J Error during creating TwitterYserCrawler");
            LOGGER.error(te.getMessage(), te);
            te.printStackTrace();
            return;
        }
        twitterCrawler.runCrawl(TwitterId, crawledDataType, crudRepository);
        synchronizationProducer.synchronize(crawledDataType, TwitterId);
    }
}

