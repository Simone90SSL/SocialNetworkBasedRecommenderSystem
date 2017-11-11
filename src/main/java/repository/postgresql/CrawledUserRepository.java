package repository.postgresql;

import domain.CrawledUser;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.repository.CrudRepository;

import java.util.List;

public interface CrawledUserRepository extends CrudRepository<CrawledUser, Long> {

    List<CrawledUser> findByUsercrawlstatus(int usercrawlstatus);

    List<CrawledUser> findByTweetscrawlstatus(int tweetscrawlstatus);

    List<CrawledUser> findByFollowingcrawlstatus(int followingcrawlstatus);

    Page<CrawledUser> findAll(Pageable pageable);
}
