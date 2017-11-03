package controller;

import domain.CrawledUser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import repository.postgresql.CrawledUserRepository;
import twitter4j.TwitterException;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * Created by simonecaldaro on 16/09/2017.
 */
@RestController
public class CrawledUserController {

    @Autowired
    private CrawledUserRepository crawledUserRepository;

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/crawleduser/{id}", method = GET)
    public CrawledUser getUserDetail(@PathVariable String id){
        return crawledUserRepository.findOne(Long.parseLong(id));
    }

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/crawledusers", method = GET)
    public List<CrawledUser> getCrawledUsers(){
        ArrayList<CrawledUser> crawledUsers = new ArrayList<>();
        for (CrawledUser cu: crawledUserRepository.findAll()){
            crawledUsers.add(cu);
        }
        return crawledUsers;
    }
}