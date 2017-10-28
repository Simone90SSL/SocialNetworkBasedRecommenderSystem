package crawler;

import crawler.user.frontier.producer.UserFrontierProducer;
import twitter4j.TwitterException;

public class TwitterCralwerFactory {

    private static TwitterCrawler twitterCrawler;

    private TwitterCralwerFactory(){

    }

    public static TwitterCrawler getTwitterCrawler(CrawlerContextConfiguarion conf, UserFrontierProducer userFrontierProducer) throws TwitterException {
        if (twitterCrawler == null){
            twitterCrawler = new TwitterCrawler(conf, userFrontierProducer);
        }
        return twitterCrawler;
    }
}
