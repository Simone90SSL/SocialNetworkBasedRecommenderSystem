package domain;

public class User {

    private long twitterId;
    private String name;
    private String nickname;
    private String location;
    private String url;


    public User(long twitterId, String name, String nickname, String location, String url) {
        this.twitterId = twitterId;
        this.name = name;
        this.nickname = nickname;
        this.location = location;
        this.url = url;
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
