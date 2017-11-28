package crawler.tweets;

import crawler.TwitterCrawler;
import domain.CrawledData;
import domain.CrawledTweets;
import frontier.FrontierProducer;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledTweetsRepository;
import twitter4j.*;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class TwitterTweetsCrawler extends TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterTweetsCrawler.class);

    private CrawledTweetsRepository crawledTweetsRepository;
    private FrontierProducer frontierProducer;

    public TwitterTweetsCrawler(
            TweetsCrawlerContextConfiguration tweetsCrawlerContextConfiguration,
            CrawledTweetsRepository crawledTweetsRepository,
            FrontierProducer frontierProducer)
            throws TwitterException{

        super(tweetsCrawlerContextConfiguration);

        this.crawledTweetsRepository = crawledTweetsRepository;
        this.frontierProducer = frontierProducer;
    }

    @Override
    public String retrieveData(CrawledData crawledData) throws TwitterException, InterruptedException {
        boolean retry;
        int number_retry = 0;
        int secondsUntilReset;

        CrawledTweets crawledTweets = (CrawledTweets)crawledData;
        crawledTweets.setLastCrawl(new java.sql.Date(new Date().getTime()));
        crawledTweets.setCrawlStatus(TwitterCrawler.CRAWLING_RUN);
        crawledTweetsRepository.save(crawledTweets);

        LOGGER.info("Start gathering tweets of the user '{}'", crawledTweets.getTwitterID());
        List<Status> statuses = null;
        do{
            try {
                Paging paging = new Paging(1, 200);
                statuses = twitter.getUserTimeline(crawledTweets.getTwitterID(), paging);
                retry = false;
            } catch (TwitterException te){
                if (te.getStatusCode() == 429
                        && number_retry < TwitterCrawler.MAX_RETRY){
                    retry = true;
                    number_retry++;
                    secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                    LOGGER.info("Twitter Exception: 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                    Thread.sleep(secondsUntilReset*1000);
                    LOGGER.info("WAKE UP and RETRY");
                } else if(te.getStatusCode() == 404 || te.getStatusCode() == 401){
                    // The URI requested is invalid or the resource requested, such as a user, does not exists.
                    // Also returned when the requested format is not supported by the requested method.
                    crawledTweets.setCrawlStatus(TwitterCrawler.CRAWLING_NOT_FOUND);
                    crawledTweetsRepository.save(crawledTweets);
                    return "";
                } else if(te.getStatusCode() == 130){
                    // The Twitter servers are up, but overloaded with requests. Try again later.
                    crawledTweets.setCrawlStatus(TwitterCrawler.CRAWLING_ERROR);
                    crawledTweetsRepository.save(crawledTweets);
                    frontierProducer.sendTweets(""+crawledTweets.getTwitterID());
                    return "";
                } else{
                    throw te;
                }
            }
        } while (retry);
        String twitterTweetsJSON = "[";
        for (Status s: statuses){
            // Saving the crawled user
            HashMap<String, String> twitterTweetsMap = new HashMap<>();
            twitterTweetsMap.put("TEXT", Optional.ofNullable(s.getText()).orElse(""));
            twitterTweetsMap.put("CREATEDAT", s.getCreatedAt().toString());
            twitterTweetsMap.put("GEOLOCATION", s.getGeoLocation()!=null?s.getGeoLocation().toString():"");
            twitterTweetsMap.put("ID", "" + s.getId());
            twitterTweetsMap.put("LANG", Optional.ofNullable(s.getLang()).orElse(""));
            twitterTweetsJSON += new JSONObject(twitterTweetsMap).toString()+",";
        }
        twitterTweetsJSON += "]";
        return twitterTweetsJSON;
    }
}
