package repository.postgresql;

import domain.CrawledUser;
import org.springframework.data.repository.CrudRepository;

public interface CrawledUserRepository extends CrudRepository<CrawledUser, Long> {
}
