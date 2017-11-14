package crawler.following;

import crawler.TwitterCrawler;
import domain.CrawledData;
import domain.CrawledFollowing;
import frontier.producer.FrontierProducer;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledFollowingRepository;
import twitter4j.IDs;
import twitter4j.TwitterException;

import java.util.Date;

public class TwitterFollowingCrawler extends TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterFollowingCrawler.class);

    private FrontierProducer frontierProducer;
    private CrawledFollowingRepository crawledFollowingRepository;

    public TwitterFollowingCrawler(
            FollowingCrawlerContextConfiguration followingCrawlerContextConfiguration,
            FrontierProducer frontierProducer,
            CrawledFollowingRepository crawledFollowingRepository)
            throws TwitterException{
        super(followingCrawlerContextConfiguration);
        this.frontierProducer = frontierProducer;
        this.crawledFollowingRepository = crawledFollowingRepository;
    }

    @Override
    public String startCrawl(CrawledData crawledData) throws TwitterException, InterruptedException {
        boolean retry;
        int number_retry = 0;
        int secondsUntilReset;

        CrawledFollowing crawledFollowing = (CrawledFollowing)crawledData;

        crawledFollowing.setLastCrawl(new java.sql.Date(new Date().getTime()));
        crawledFollowing.setCrawlStatus(TwitterCrawler.CRAWLING_RUN);
        crawledFollowingRepository.save(crawledFollowing);

        LOGGER.info("Start gathering following of the user '{}'", crawledFollowing.getTwitterID());
        long cursor = -1;
        IDs followed = null;
        String followingCrawled = "";

        // Iterate over the followers
        do {
            do{
                try {
                    followed = twitter.getFriendsIDs(crawledFollowing.getTwitterID(), cursor);
                    retry = false;
                } catch (TwitterException te){
                    if (te.getStatusCode() == 429 && number_retry < TwitterCrawler.MAX_RETRY){
                        retry = true;
                        number_retry++;
                        secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                        LOGGER.info("Twitter Exception: 'Rate limit exceeded' --> wait '{}' secs", secondsUntilReset);
                        Thread.sleep(secondsUntilReset*1000);
                        LOGGER.info("WAKE UP and RETRY");
                    } else{
                        throw te;
                    }
                }
            }while (retry);

            for (long f: followed.getIDs()){
                followingCrawled+=f+",";
                // Adding the user found to the FRONTIER
                LOGGER.debug("Add twitter user id into the frontier, with twitter-id='{}'", f);
                frontierProducer.sendUser("" + f);
            }
        } while ((cursor = followed.getNextCursor()) != 0);
        return followingCrawled;
    }
}
