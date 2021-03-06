package domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Date;

@Entity
@Table(name = "crawleduser")
public class CrawledUser implements CrawledData {

    @Id
    @Column(name = "twitterid")
    private long TwitterID;

    @Column(name = "lastcrawl")
    private Date lastcrawl;

    @Column(name = "crawlstatus")
    private int crawlstatus;

    @Column(name = "datacrawled")
    private String datacrawled;

    public CrawledUser(long TwitterID){
        this.TwitterID = TwitterID;
    }

    public CrawledUser(){}

    public long getTwitterID() {
        return TwitterID;
    }

    public void setTwitterID(long twitterID) {
        TwitterID = twitterID;
    }

    public Date getLastCrawl() {
        return lastcrawl;
    }

    public void setLastCrawl(Date lastCrawl) {
        this.lastcrawl = lastCrawl;
    }

    public int getCrawlStatus() {
        return crawlstatus;
    }

    public void setCrawlStatus(int crawlStatus) {
        this.crawlstatus = crawlStatus;
    }

    @Override
    public boolean isDataCrawledEmpty() {
        return datacrawled == null || datacrawled.isEmpty() || "{}".equals(datacrawled);
    }

    public String getDataCrawled() {
        return datacrawled;
    }

    public void setDataCrawled(String dataCrawled) {
        this.datacrawled = dataCrawled;
    }
}
