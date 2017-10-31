package domain;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import java.sql.Date;

@Entity
@Table(name = "crawleduser")
public class CrawledUser {

    @Id
    @Column(name = "twitterid")
    private long TwitterID;

    @Column(name = "lastusercrawl")
    private Date lastUserCrawl;

    @Column(name = "lastfollowingcrawl")
    private Date lastFollowingCrawl;

    @Column(name = "lasttweetscrawl")
    private Date lastTweetsCrawl;

    @Column(name = "status")
    private int status;

    public CrawledUser(){}

    public long getTwitterID() {
        return TwitterID;
    }

    public Date getLastUserCrawl() {
        return lastUserCrawl;
    }

    public Date getLastFollowingCrawl() {
        return lastFollowingCrawl;
    }

    public Date getLastTweetsCrawl() {
        return lastTweetsCrawl;
    }

    public int getStatus() {
        return status;
    }

    public void setTwitterID(long twitterID) {
        TwitterID = twitterID;
    }

    public void setLastUserCrawl(Date lastUserCrawl) {
        this.lastUserCrawl = lastUserCrawl;
    }

    public void setLastFollowingCrawl(Date lastFollowingCrawl) {
        this.lastFollowingCrawl = lastFollowingCrawl;
    }

    public void setLastTweetsCrawl(Date lastTweetsCrawl) {
        this.lastTweetsCrawl = lastTweetsCrawl;
    }

    public void setStatus(int status) {
        this.status = status;
    }
}

