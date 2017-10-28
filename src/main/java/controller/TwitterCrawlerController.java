package controller;

import crawler.CrawlerContextConfiguarion;
import crawler.TwitterCrawler;
import crawler.user.frontier.producer.UserFrontierProducer;
import domain.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import twitter4j.TwitterException;

import javax.jws.soap.SOAPBinding;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

@RestController
@EnableAsync
public class TwitterCrawlerController {


    @Autowired
    private CrawlerContextConfiguarion conf;

    @Autowired
    private UserFrontierProducer userFrontierProducer;

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/crawlTwitterUser/{TwitterId}", method = GET)
    public List<User> getProductDetail(@PathVariable String TwitterId){
        System.out.println(conf.getConsumerKey());
        try {
            TwitterCrawler tc = new TwitterCrawler(conf, userFrontierProducer);
            return tc.getFollowed(Long.parseLong(TwitterId));
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        return null;
    }
}