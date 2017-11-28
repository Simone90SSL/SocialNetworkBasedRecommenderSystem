package synchronization;

import crawler.TwitterCrawler;
import domain.CrawledData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;

@Component
public class SynchronizationProducer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizationProducer.class);

    private static final String SYNC_USER_TOPIC = "usertransactiona";
    private static final String SYNC_FOLLOWING_TOPIC = "followingtransactiona";
    private static final String SYNC_TWEETS_TOPIC = "tweetstransactiona";

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private CrawledUserRepository crawledUserRepository;
    @Autowired
    private CrawledFollowingRepository crawledFollowingRepository;
    @Autowired
    private CrawledTweetsRepository crawledTweetsRepository;

    public void synchronize(TwitterCrawler.CRAWLED_DATA_TYPE crawledDataType, String twitterId){
        String synchronizationTopic = "";
        CrudRepository crudRepository = null;
        if (crawledDataType == TwitterCrawler.CRAWLED_DATA_TYPE.USER){
            synchronizationTopic = SYNC_USER_TOPIC;
            crudRepository = crawledUserRepository;
        } else if(crawledDataType == TwitterCrawler.CRAWLED_DATA_TYPE.FOLLOWING){
            synchronizationTopic = SYNC_FOLLOWING_TOPIC;
            crudRepository = crawledFollowingRepository;
        } else if(crawledDataType == TwitterCrawler.CRAWLED_DATA_TYPE.TWEETS){
            synchronizationTopic = SYNC_TWEETS_TOPIC;
            crudRepository = crawledTweetsRepository;
        } else{
            throw new RuntimeException("DATA TYPE SYNC NOT SUPPORTED");
        }

        sendSynch(synchronizationTopic, Long.parseLong(twitterId), crudRepository);

    }

    private void sendSynch(String topic, long twitterId, CrudRepository crudRepository){
        LOGGER.info("Sending data of user id='{}' to topic='{}'", twitterId, topic);
        CrawledData crawledData = (CrawledData) crudRepository.findOne(twitterId);

        if (crawledData == null){
            LOGGER.warn("CrawledData '{}' not found");
            return;
        }

        String dataToSync = crawledData.getDataCrawled();
        if (crawledData.isDataCrawledEmpty()){
            LOGGER.warn("Empty data '{}': '{}' not to synk", dataToSync, twitterId);
            crawledData.setCrawlStatus(TwitterCrawler.NOTHING_TO_SYNC);
        } else{
            crawledData.setCrawlStatus(TwitterCrawler.SYNC_INIT);
            String payload = crawledData.getTwitterID()+":"+dataToSync;
            kafkaTemplate.send(topic, payload);
        }
        crudRepository.save(crawledData);
    }
}



