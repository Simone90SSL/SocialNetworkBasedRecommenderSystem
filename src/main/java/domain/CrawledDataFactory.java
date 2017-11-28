package domain;

import crawler.TwitterCrawler.CRAWLED_DATA_TYPE;

public class CrawledDataFactory {

    public static CrawledData getInstance(CRAWLED_DATA_TYPE type, long twitterId){
        if (type == CRAWLED_DATA_TYPE.FOLLOWING){
            return new CrawledFollowing(twitterId);
        } else if (type == CRAWLED_DATA_TYPE.TWEETS){
            return new CrawledTweets(twitterId);
        } else if (type == CRAWLED_DATA_TYPE.USER){
            return new CrawledUser(twitterId);
        } else{
            throw new RuntimeException("CRAWLED_DATA_TYPE not correct");
        }

    }
}
