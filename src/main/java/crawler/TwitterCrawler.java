package crawler;

import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

public abstract class TwitterCrawler{

    public static final int CRAWLING_INIT = 0;
    public static final int CRAWLING_WAITING = 1;
    public static final int CRAWLING_RUN = 2;
    public static final int CRAWLING_END = 3;
    public static final int CRAWLING_ERROR = 7;
    public static final int CRAWLING_NOT_FOUND = 9;
    public static final int SYNC_INIT = 4;
    public static final int SYNC_TERMINATED = 5;
    public static final int SYNC_TERMINATED_ERR = 6;
    public static final int NOTHING_TO_SYNC = 8;

    public static final int MAX_RETRY = 3;

    protected Twitter twitter;

    public TwitterCrawler(CrawlerContextConfiguration crawlerContextConfiguration) throws TwitterException {

        //Instantiate a re-usable and thread-safe factory
        TwitterFactory twitterFactory = new TwitterFactory();

        //Instantiate a new Twitter instance
        twitter = twitterFactory.getInstance();

        //setup OAuth Consumer Credentials
        twitter.setOAuthConsumer(
                crawlerContextConfiguration.getConsumerKey(),
                crawlerContextConfiguration.getConsumerSecret());

        //setup OAuth Access Token
        twitter.setOAuthAccessToken(new AccessToken(
                crawlerContextConfiguration.getAccessToken(),
                crawlerContextConfiguration.getAccessTokenSecret()));

        User u = twitter.verifyCredentials();
        if (u == null){
            throw new TwitterException("Impossible to access on Twitter API with configuration: "
                    + crawlerContextConfiguration.getName());
        }
    }
}

