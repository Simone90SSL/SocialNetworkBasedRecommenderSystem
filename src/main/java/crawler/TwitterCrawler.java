package crawler;

import domain.CrawledData;
import domain.CrawledDataFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.repository.CrudRepository;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.User;
import twitter4j.auth.AccessToken;

import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

public abstract class TwitterCrawler implements Runnable{

    private static final Logger LOGGER = LoggerFactory.getLogger(TwitterCrawler.class);

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

    public enum CRAWLED_DATA_TYPE  { FOLLOWING, TWEETS, USER };

    protected Twitter twitter;
    private String name;

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

        this.name = crawlerContextConfiguration.getName();

        User u = twitter.verifyCredentials();
        if (u == null){
            throw new TwitterException("Impossible to access on Twitter API with configuration: "
                    + crawlerContextConfiguration.getName());
        }
    }

    public static boolean isToCrawl(CrawledData crawledData){

        if (crawledData.getLastCrawl() != null){
            // User is in the DB --> check if it must be crawled
            long diff = (new Date()).getTime() - crawledData.getLastCrawl().getTime();
            if (TimeUnit.DAYS.convert(diff, TimeUnit.MILLISECONDS) <= 7){
                LOGGER.info("twitter-id='{}' has recently been crawled --> SKIP IT", crawledData.getTwitterID());
                return false;
            }

            if(!crawledData.isDataCrawledEmpty() && crawledData.getCrawlStatus() == TwitterCrawler.SYNC_TERMINATED){
                LOGGER.warn("twitter-id='{}' has to sync old cralwed data", crawledData.getTwitterID());
                return false;
            }
        }

        return true;
    }

    public synchronized void runCrawl(String twitterIdStr,
                                      CRAWLED_DATA_TYPE crawledDataType,
                                      CrudRepository crudRepository){

        LOGGER.info("Init crawling '{}' for twitter id '{}'", crawledDataType, twitterIdStr);
        long twitterId = Long.parseLong(twitterIdStr);

        // Check that twitter user is not present, or it has been crawled at least 24h ago
        CrawledData crawledData = (CrawledData) Optional
                .ofNullable(crudRepository.findOne(twitterId))
                .orElse(CrawledDataFactory.getInstance(crawledDataType, twitterId));

        if(!TwitterCrawler.isToCrawl(crawledData)){
            return;
        }

        crawledData.setCrawlStatus(TwitterCrawler.CRAWLING_WAITING);
        crudRepository.save(crawledData);

        // User must be crawled
        LOGGER.info("Start crawling user with twitter-id='{}', getting the '{}'", twitterId, crawledDataType);
        try {
            String result = retrieveData(crawledData);
            crawledData.setCrawlStatus(TwitterCrawler.CRAWLING_END);
            crawledData.setDataCrawled(result);
            crudRepository.save(crawledData);
            return;
        } catch (TwitterException te){
            LOGGER.error("Twitter4J Error during twitter '{}' crawling with input '{}'",
                    crawledDataType, crawledData.getTwitterID());
            LOGGER.error(te.getMessage(), te);
            te.printStackTrace();
        } catch (InterruptedException ie) {
            LOGGER.error("Interrupted Error during twitter '{}' crawling with input '{}'",
                    crawledDataType, crawledData.getTwitterID());
            LOGGER.error(ie.getMessage(), ie);
            ie.printStackTrace();
        } catch (Exception e){
            LOGGER.error("Error during twitter '{}' crawling with input '{}'",
                    crawledDataType, crawledData.getTwitterID());
            LOGGER.error(e.getMessage(), e);
            e.printStackTrace();
        }

        // If code reach this point, the crawl has terminated in error
        crawledData.setCrawlStatus(TwitterCrawler.CRAWLING_ERROR);
        crudRepository.save(crawledData);
    }

    public abstract String retrieveData(CrawledData crawledData) throws TwitterException, InterruptedException;

    private static boolean compareCrawlType(String type, CRAWLED_DATA_TYPE crawledDataType){
        return type!=null && crawledDataType.toString().toLowerCase().equals(type.toLowerCase());
    }

    public static boolean isCrawlerFollowing(String type){
        return compareCrawlType(type, CRAWLED_DATA_TYPE.FOLLOWING);
    }

    public static boolean isCrawlerTweets(String type){
        return compareCrawlType(type, CRAWLED_DATA_TYPE.TWEETS);
    }

    public static boolean isCrawlerUser(String type){
        return compareCrawlType(type, CRAWLED_DATA_TYPE.USER);
    }



    @Override
    public void run(){

    }
}



