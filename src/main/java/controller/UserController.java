package controller;

import crawler.following.FollowingCrawlerContextConfiguration;
import crawler.TwitterCralwerFactory;
import crawler.following.TwitterFollowingCrawler;
import crawler.following.frontier.producer.FollowingFrontierProducer;
import domain.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import repository.postgresql.CrawledUserRepository;
import repository.neo4j.UserRepository;
import twitter4j.TwitterException;

import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * Created by simonecaldaro on 16/09/2017.
 */
@RestController
public class UserController {

    @Autowired
    private FollowingCrawlerContextConfiguration followingCrawlerContextConfiguration;

    @Autowired
    private FollowingFrontierProducer followingFrontierProducer;

    @Autowired
    private UserRepository userRepository;

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/users", method = GET)
    public List<User> getUsers(){
        return null;
    }

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/user/{id}", method = GET)
    public User getUserDetail(@PathVariable String id){
        return null;
    }

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/followed", method = GET)
    public List<User> getFollowed(){
        try {
            TwitterFollowingCrawler tc =TwitterCralwerFactory.getTwitterCrawler(
                    followingCrawlerContextConfiguration,
                    followingFrontierProducer,
                    userRepository,
                    crawledUserRepository);
            tc.crawlFollowedByTwitterUserId(-1L);
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        return null;
    }
}

