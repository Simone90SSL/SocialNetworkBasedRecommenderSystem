package crawler;

import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.following.TwitterFollowingCrawler;
import crawler.tweets.TweetsCrawlerContextConfiguration;
import crawler.tweets.TwitterTweetsCrawler;
import crawler.user.TwitterUserCrawler;
import crawler.user.UserCrawlerContextConfiguration;
import domain.CrawledDataFactory;
import frontier.FrontierProducer;
import org.springframework.data.repository.CrudRepository;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;
import twitter4j.TwitterException;

import java.util.HashSet;

public class TwitterCralwerFactory {

    private static TwitterFollowingCrawler twitterFollowingCrawler;
    private static TwitterUserCrawler twitterUserCrawler;
    private static TwitterTweetsCrawler twitterTweetsCrawler;

    private TwitterCralwerFactory(){

    }

    public static TwitterCrawler getInstance(TwitterCrawler.CRAWLED_DATA_TYPE crawledDataType,
                                             CrawlerContextConfiguration crawlerContextConfiguration,
                                             FrontierProducer frontierProducer,
                                             CrudRepository crudRepository) throws TwitterException{

        if (crawledDataType == TwitterCrawler.CRAWLED_DATA_TYPE.FOLLOWING){
            return getTwitterFollowingCrawler(
                    (FollowingCrawlerContextConfiguration) crawlerContextConfiguration,
                    frontierProducer,
                    (CrawledFollowingRepository) crudRepository);
        } else if(crawledDataType == TwitterCrawler.CRAWLED_DATA_TYPE.TWEETS){
            return getTwitterTweetsCrawler(
                    (TweetsCrawlerContextConfiguration) crawlerContextConfiguration,
                    (CrawledTweetsRepository) crudRepository,
                    frontierProducer);
        } else if(crawledDataType == TwitterCrawler.CRAWLED_DATA_TYPE.USER){
            return getTwitterUserCrawler(
                    (UserCrawlerContextConfiguration) crawlerContextConfiguration,
                    (CrawledUserRepository) crudRepository,
                    frontierProducer);
        } else{
            throw new RuntimeException("CRAWLED DATA TYPE not found: "+crawledDataType);
        }
    }

    public static TwitterFollowingCrawler getTwitterFollowingCrawler(
            CrawlerContextConfiguration conf,
            FrontierProducer frontierProducer,
            CrawledFollowingRepository crawledFollowingRepository)
            throws TwitterException {

        if (twitterFollowingCrawler == null){
            twitterFollowingCrawler = new TwitterFollowingCrawler(
                    conf,
                    frontierProducer,
                    crawledFollowingRepository);
        }

        return twitterFollowingCrawler;
    }

    public static TwitterUserCrawler getTwitterUserCrawler(
            UserCrawlerContextConfiguration conf,
            CrawledUserRepository crawledUserRepository,
            FrontierProducer frontierProducer)
            throws TwitterException {

        if (twitterUserCrawler == null){
            twitterUserCrawler = new TwitterUserCrawler(
                    conf,
                    crawledUserRepository,
                    frontierProducer);
        }

        return twitterUserCrawler;
    }

    public static TwitterTweetsCrawler getTwitterTweetsCrawler(
            TweetsCrawlerContextConfiguration tweetsCrawlerContextConfiguration,
            CrawledTweetsRepository crawledTweetsRepository,
            FrontierProducer frontierProducer) throws TwitterException {

        if (twitterTweetsCrawler == null){
            twitterTweetsCrawler = new TwitterTweetsCrawler(
                    tweetsCrawlerContextConfiguration,
                    crawledTweetsRepository,
                    frontierProducer);
        }

        return twitterTweetsCrawler;
    }
}
