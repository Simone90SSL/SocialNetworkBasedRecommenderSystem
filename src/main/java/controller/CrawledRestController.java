package controller;

import crawler.TwitterCrawler;
import domain.*;
import frontier.FrontierProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;
import synchronization.SynchronizationProducer;

import java.util.ArrayList;
import java.util.List;

import static org.springframework.web.bind.annotation.RequestMethod.GET;

/**
 * Created by simonecaldaro on 16/09/2017.
 */
@RestController
@RequestMapping("/crawler")
public class CrawledRestController {

    private static final int PAGE_SIZE = 100;

    @Autowired
    private CrawledUserRepository crawledUserRepository;
    @Autowired
    private CrawledTweetsRepository crawledTweetRepository;
    @Autowired
    private CrawledFollowingRepository crawledFollowingRepository;
    @Autowired
    private FrontierProducer frontierProducer;
    @Autowired
    private SynchronizationProducer synchronizationProducer;

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/user/{id}", method = GET)
    public CrawledUser getUserDetail(@PathVariable String id){
        return crawledUserRepository.findOne(Long.parseLong(id));
    }

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/sync/tweets/{status}", method = GET)
    public String reSink(@PathVariable String status) {

        int statusInt = 0;
        try {
            statusInt = Integer.parseInt(status);
        } catch (Exception e){
            return e.getMessage();
        }

        int count = 0;
        int page = 0;
        Page<CrawledData> crawledDataPage = null;
        do{
            crawledDataPage = crawledTweetRepository.findByCrawlstatus(statusInt, new PageRequest(page++, PAGE_SIZE));
            for (CrawledData crawledData: crawledDataPage){
                if(crawledData.getCrawlStatus() == TwitterCrawler.SYNC_INIT){
                    synchronizationProducer.synchronize(TwitterCrawler.CRAWLED_DATA_TYPE.TWEETS, ""+crawledData.getTwitterID());
                    count++;
                }
            }
        } while(crawledDataPage.hasNext());
        return "Sent "+count+" tweets synchronization";
    }

    @RequestMapping(produces = MediaType.APPLICATION_JSON_UTF8_VALUE, value = "/users", method = GET)
    public List<Long> getCrawledUsers(){
        return null;
    }
}