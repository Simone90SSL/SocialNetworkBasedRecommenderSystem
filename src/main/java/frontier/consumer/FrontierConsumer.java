package frontier.consumer;

import crawler.CrawlerContextConfiguration;
import crawler.TwitterCralwerFactory;
import crawler.TwitterCrawler;
import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.following.TwitterFollowingCrawler;
import crawler.tweets.TweetsCrawlerContextConfiguration;
import crawler.tweets.TwitterTweetsCrawler;
import crawler.user.TwitterUserCrawler;
import crawler.user.UserCrawlerContextConfiguration;
import domain.*;
import frontier.producer.FrontierProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;
import twitter4j.Twitter;
import twitter4j.TwitterException;

import java.util.Optional;

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

    @KafkaListener(id = "followingC", topics = "${kafka.topic.followingfrontier}")
    public void receiveFollowing(String TwitterId) {
        receive(TwitterId,
                CrawledDataFactory.CRAWLED_DATA_TYPE.FOLLOWING,
                followingCrawlerContextConfiguration,
                crawledFollowingRepository);
    }

    @KafkaListener(id = "tweetsC", topics = "${kafka.topic.tweetsfrontier}")
    public void receiveTweets(String TwitterId) {
        receive(TwitterId,
                CrawledDataFactory.CRAWLED_DATA_TYPE.TWEETS,
                tweetsCrawlerContextConfiguration,
                crawledTweetsRepository);
    }

    @KafkaListener(id = "userC", topics = "${kafka.topic.userfrontier}")
    public void receiveUser(String TwitterId) {
        receive(TwitterId,
                CrawledDataFactory.CRAWLED_DATA_TYPE.USER,
                userCrawlerContextConfiguration,
                crawledUserRepository);
    }

    public void receive(String TwitterId,
                        CrawledDataFactory.CRAWLED_DATA_TYPE crawledDataType,
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
        twitterCrawler.initCrawl(TwitterId, crawledDataType, crudRepository);
    }

    @KafkaListener(id = "followingTC", topics = "${kafka.topic.followingtransactionb}")
    public void receiveFollowingTransaction(String transactionResponse) {
        LOGGER.info("Following Transaction terminated '{}'", transactionResponse);
        finaliseTransaction(transactionResponse, crawledFollowingRepository);
    }

    @KafkaListener(topics = "${kafka.topic.tweetstransactionb}")
    public void receiveTweetsTransaction(String transactionResponse) {
        LOGGER.info("Tweets Transaction terminated '{}'", transactionResponse);
        finaliseTransaction(transactionResponse, crawledTweetsRepository);
    }

    @KafkaListener(topics = "${kafka.topic.usertransactionb}")
    public void receiveUserTransaction(String transactionResponse) {
        LOGGER.info("User Transaction terminated '{}'", transactionResponse);
        finaliseTransaction(transactionResponse, crawledUserRepository);
    }

    public static void finaliseTransaction(String transactionResponse, CrudRepository crudRepository){
        long TwitterId = Long.parseLong(transactionResponse.split(",")[0]);
        String status = transactionResponse.split(",")[1];

        CrawledData crawledData = (CrawledData) crudRepository.findOne(TwitterId);
        if (status.equals("OK")){
            crawledData.setCrawlStatus(TwitterCrawler.SYNC_TERMINATED);
            //crawledUser.setUsercrawled("");
        } else{
            crawledData.setCrawlStatus(TwitterCrawler.SYNC_TERMINATED_ERR);
        }

        crudRepository.save(crawledData);
    }
}