package crawler;

import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.following.TwitterFollowingCrawler;
import crawler.following.frontier.producer.FollowingFrontierProducer;
import crawler.user.frontier.TwitterUserCrawler;
import crawler.user.frontier.UserCrawlerContextConfiguration;
import crawler.user.frontier.producer.UserFrontierProducer;
import repository.postgresql.CrawledUserRepository;
import transaction.following.producer.FollowingTransactionProducer;
import transaction.user.producer.UserTransactionProducer;
import twitter4j.TwitterException;

public class TwitterCralwerFactory {

    private static TwitterFollowingCrawler twitterFollowingCrawler;
    private static TwitterUserCrawler twitterUserCrawler;

    private TwitterCralwerFactory(){

    }

    public static TwitterFollowingCrawler getTwitterFollowingCrawler(
            FollowingCrawlerContextConfiguration conf,
            UserFrontierProducer userFrontierProducer,
            CrawledUserRepository crawledUserRepository,
            FollowingTransactionProducer followingTransactionProducer)
            throws TwitterException {

        if (twitterFollowingCrawler == null){
            twitterFollowingCrawler = new TwitterFollowingCrawler(
                    conf,
                    userFrontierProducer,
                    crawledUserRepository,
                    followingTransactionProducer);
        }

        return twitterFollowingCrawler;
    }

    public static TwitterUserCrawler getTwitterUserCrawler(
            UserCrawlerContextConfiguration conf,
            UserFrontierProducer userFrontierProducer,
            CrawledUserRepository crawledUserRepository,
            UserTransactionProducer userTransactionProducer)
            throws TwitterException {

        if (twitterUserCrawler == null){
            twitterUserCrawler = new TwitterUserCrawler(
                    conf,
                    userFrontierProducer,
                    crawledUserRepository,
                    userTransactionProducer);
        }

        return twitterUserCrawler;
    }
}
