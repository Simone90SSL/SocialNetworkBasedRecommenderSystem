package synchronization;

import crawler.TwitterCrawler;
import domain.CrawledData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.repository.CrudRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import repository.postgresql.CrawledFollowingRepository;
import repository.postgresql.CrawledTweetsRepository;
import repository.postgresql.CrawledUserRepository;

@Component
public class SynchronizationConsumer {
    private static final Logger LOGGER = LoggerFactory.getLogger(SynchronizationConsumer.class);

    @Autowired
    private CrawledUserRepository crawledUserRepository;
    @Autowired
    private CrawledFollowingRepository crawledFollowingRepository;
    @Autowired
    private CrawledTweetsRepository crawledTweetsRepository;

    @KafkaListener(topics = "${kafka.topic.followingtransactionb}")
    public void receiveFollowingTransaction(String transactionResponse) {
        LOGGER.info("Following Transaction terminated '{}'", transactionResponse);
        finaliseTransaction(transactionResponse, crawledFollowingRepository);
    }

    @KafkaListener(topics = "${kafka.topic.tweetstransactionb}")
    public void receiveTweetsTransaction(String transactionResponse) {
        LOGGER.info("Tweets Transaction terminated '{}'", transactionResponse);
        finaliseTransaction(transactionResponse, crawledTweetsRepository);
    }

    @KafkaListener(topics = "${kafka.topic.usertransactionb}")
    public void receiveUserTransaction(String transactionResponse) {
        LOGGER.info("User Transaction terminated '{}'", transactionResponse);
        finaliseTransaction(transactionResponse, crawledUserRepository);
    }

    private static void finaliseTransaction(String transactionResponse, CrudRepository crudRepository){
        long TwitterId = Long.parseLong(transactionResponse.split(",")[0]);
        String status = transactionResponse.split(",")[1];

        CrawledData crawledData = (CrawledData) crudRepository.findOne(TwitterId);
        if (status.equals("OK")){
            crawledData.setCrawlStatus(TwitterCrawler.SYNC_TERMINATED);
            crawledData.setDataCrawled("");
        } else{
            crawledData.setCrawlStatus(TwitterCrawler.SYNC_TERMINATED_ERR);
        }
        crudRepository.save(crawledData);
    }
}

