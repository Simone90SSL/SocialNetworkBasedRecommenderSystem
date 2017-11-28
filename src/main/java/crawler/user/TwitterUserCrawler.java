package crawler.user;

import crawler.TwitterCrawler;
import domain.CrawledData;
import domain.CrawledUser;
import frontier.FrontierProducer;
import org.slf4j.LoggerFactory;
import repository.postgresql.CrawledUserRepository;
import twitter4j.*;

import java.util.Date;
import java.util.HashMap;
import java.util.Optional;

public class TwitterUserCrawler extends TwitterCrawler {

    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(TwitterUserCrawler.class);

    private CrawledUserRepository crawledUserRepository;
    private FrontierProducer frontierProducer;

    public TwitterUserCrawler(
            UserCrawlerContextConfiguration userCrawlerContextConfiguration,
            CrawledUserRepository crawledUserRepository,
            FrontierProducer frontierProducer)
            throws TwitterException{

        super(userCrawlerContextConfiguration);
        this.crawledUserRepository = crawledUserRepository;
        this.frontierProducer = frontierProducer;
    }

    @Override
    public String retrieveData(CrawledData crawledData) throws TwitterException, InterruptedException {

        // Trigger crawling of FOLLOWING / TWEETS
        frontierProducer.sendFollowing(""+crawledData.getTwitterID());
        frontierProducer.sendTweets(""+crawledData.getTwitterID());

        boolean retry = false;
        int number_retry = 0;
        int secondsUntilReset;

        CrawledUser crawledUser = (CrawledUser)crawledData;

        crawledUser.setLastCrawl(new java.sql.Date(new Date().getTime()));
        crawledUser.setCrawlStatus(TwitterCrawler.CRAWLING_RUN);
        crawledUserRepository.save(crawledUser);

        LOGGER.info("Start gathering information of the user '{}'", crawledUser.getTwitterID());
        User currentTwitterUser = null;
        do{
            try {
                currentTwitterUser = twitter.showUser(crawledUser.getTwitterID());
                retry = false;
            } catch (TwitterException te){
                if (( te.getStatusCode() == 429 || te.getStatusCode() == 403 )
                        && number_retry < TwitterCrawler.MAX_RETRY){
                    retry = true;
                    number_retry++;
                    secondsUntilReset = te.getRateLimitStatus().getSecondsUntilReset();
                    LOGGER.info("Twitter Exception: 'Rate limit exceeded' --> must wait '{}' secs", secondsUntilReset);
                    Thread.sleep(secondsUntilReset*1000);
                    LOGGER.info("WAKE UP and RETRY");
                } else if(te.getStatusCode() == 404){
                    // The URI requested is invalid or the resource requested, such as a user, does not exists.
                    // Also returned when the requested format is not supported by the requested method.
                    crawledUser.setCrawlStatus(TwitterCrawler.CRAWLING_NOT_FOUND);
                    crawledUserRepository.save(crawledUser);
                    return "";
                } else{
                    throw te;
                }
            }
        }while (retry);

        HashMap<String, String> twitterUserMap = new HashMap<>();
        twitterUserMap.put("URL", Optional.ofNullable(currentTwitterUser.getURL()).orElse(""));
        twitterUserMap.put("LOCATION", Optional.ofNullable(currentTwitterUser.getLocation()).orElse(""));
        twitterUserMap.put("SCREENNAME", Optional.ofNullable(currentTwitterUser.getScreenName()).orElse(""));
        twitterUserMap.put("NAME", Optional.ofNullable(currentTwitterUser.getName()).orElse(""));
        twitterUserMap.put("EMAIL", Optional.ofNullable(currentTwitterUser.getEmail()).orElse(""));
        return new JSONObject(twitterUserMap).toString();
    }
}
