package crawler;

import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.following.TwitterFollowingCrawler;
import crawler.following.frontier.producer.FollowingFrontierProducer;
import repository.postgresql.CrawledUserRepository;
import repository.neo4j.UserRepository;
import twitter4j.TwitterException;

public class TwitterCralwerFactory {

    private static TwitterFollowingCrawler twitterFollowingCrawler;

    private TwitterCralwerFactory(){

    }

    public static TwitterFollowingCrawler getTwitterCrawler(
            FollowingCrawlerContextConfiguration conf,
            FollowingFrontierProducer followingFrontierProducer,
            UserRepository userRepository,
            CrawledUserRepository crawledUserRepository)
            throws TwitterException {

        if (twitterFollowingCrawler == null){
            twitterFollowingCrawler = new TwitterFollowingCrawler(
                    conf,
                    followingFrontierProducer,
                    userRepository,
                    crawledUserRepository);
        }

        return twitterFollowingCrawler;
    }
}
