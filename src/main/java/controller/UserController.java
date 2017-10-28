package controller;

import crawler.CrawlerContextConfiguarion;
import crawler.TwitterCralwerFactory;
import crawler.TwitterCrawler;
import crawler.user.frontier.producer.UserFrontierProducer;
import domain.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import twitter4j.TwitterException;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * Created by simonecaldaro on 16/09/2017.
 */
@RestController
public class UserController {

    @Autowired
    private CrawlerContextConfiguarion conf;

    @Autowired
    private UserFrontierProducer userFrontierProducer;

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
            TwitterCrawler tc =TwitterCralwerFactory.getTwitterCrawler(conf, userFrontierProducer);
            return tc.getFollowed(-1);
        } catch (TwitterException e) {
            e.printStackTrace();
        }
        return null;
    }
}

