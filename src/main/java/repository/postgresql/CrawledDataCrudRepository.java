package repository.postgresql;

import domain.CrawledData;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Query;

import java.util.List;

public interface CrawledDataCrudRepository {

    List<CrawledData> findByCrawlstatus(int crawlstatus);
    Page<CrawledData> findByCrawlstatus(int crawlstatus, Pageable page);

    Page<CrawledData> findAll(Pageable pageable);

    @Query(value = "SELECT crawlstatus, count(*)" +
            " FROM crawledtweets GROUP BY crawlstatus",
    nativeQuery = true)
    List<Object> getStatus();
}
