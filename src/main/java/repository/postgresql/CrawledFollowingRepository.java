package repository.postgresql;

import domain.CrawledFollowing;
import org.springframework.data.repository.CrudRepository;

public interface CrawledFollowingRepository extends CrudRepository<CrawledFollowing, Long>, CrawledDataCrudRepository {

}
