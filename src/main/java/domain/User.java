package domain;

import org.neo4j.ogm.annotation.GraphId;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@NodeEntity
public class User {

    @GraphId
    private Long id;

    private long twitterId;
    private String name;
    private String nickname;
    private String location;
    private String url;

    @Relationship(type = "FOLLOW")
    public Set<User> follows;

    private User() {
        // Empty constructor required as of Neo4j API 2.0.5
    };

    public User(long twitterId, String name, String nickname, String location, String url) {
        this.twitterId = twitterId;
        this.name = name;
        this.nickname = nickname;
        this.location = location;
        this.url = url;
    }

    public User(long twitterId) {
        this.twitterId = twitterId;
        this.name = "";
        this.nickname = "";
        this.location = "";
        this.url = "";
    }

    public Long getId(){
        return id;
    }

    public long getTwitterId() {
        return twitterId;
    }

    public String getName() {
        return name;
    }

    public String getNickname() {
        return nickname;
    }

    public String getLocation() {
        return location;
    }

    public String getUrl() {
        return url;
    }

    public void setTwitterId(long twitterId) {
        this.twitterId = twitterId;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNickname(String nickname) {
        this.nickname = nickname;
    }

    public void setLocation(String location) {
        this.location = location;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public void setId(Long id){
        this.id = id;
    }

    public void follow(User user) {
        if (follows == null) {
            follows = new HashSet<>();
        }
        follows.add(user);
    }

    @Override
    public String toString() {
        return "User{" +
                "twitterId=" + twitterId +
                ", name='" + name + '\'' +
                ", nickname='" + nickname + '\'' +
                ", location='" + location + '\'' +
                ", url='" + url + '\'' +
                '}';
    }
}
