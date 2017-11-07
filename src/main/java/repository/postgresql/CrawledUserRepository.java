package repository.postgresql;

import domain.CrawledUser;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface CrawledUserRepository extends CrudRepository<CrawledUser, Long> {

    List<CrawledUser> findByUsercrawlstatus(int usercrawlstatus);

    List<CrawledUser> findByTweetscrawlstatus(int tweetscrawlstatus);
}
