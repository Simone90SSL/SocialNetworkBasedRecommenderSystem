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

    @Column(name = "followingcrawlstatus")
    private int followingcrawlstatus;

    @Column(name = "followingcrawled")
    private String followingcrawled;

    @Column(name = "usercrawlstatus")
    private int usercrawlstatus;

    @Column(name = "usercrawled")
    private String usercrawled;


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


    public int getFollowingcrawlstatus() {
        return followingcrawlstatus;
    }

    public String getFollowingcrawled() {
        return followingcrawled;
    }

    public int getUsercrawlstatus() {
        return usercrawlstatus;
    }

    public String getUsercrawled() {
        return usercrawled;
    }

    public void setFollowingcrawlstatus(int followingcrawlstatus) {
        this.followingcrawlstatus = followingcrawlstatus;
    }

    public void setFollowingcrawled(String followingcrawled) {
        this.followingcrawled = followingcrawled;
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

    public void setUsercrawlstatus(int usercrawlstatus) {
        this.usercrawlstatus = usercrawlstatus;
    }

    public void setUsercrawled(String usercrawled) {
        this.usercrawled = usercrawled;
    }
}

