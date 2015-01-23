package com.jdreamer.ccp;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by bibagimon on 1/24/15.
 */
public class Session implements Serializable {
    private String session;
    private long start;
    private long end;

    private String user;
    private String kid;

    private List<String> hover = Collections.emptyList();
    private List<String> browsed = Collections.emptyList();
    private List<String> popular = Collections.emptyList();
    private List<String> searched = Collections.emptyList();
    private List<String> queued = Collections.emptyList();
    private List<String> recent = Collections.emptyList();

    private Map<String, String> played = Collections.emptyMap();

    private List<String> actions = Collections.emptyList();
    private List<String> recommended = Collections.emptyList();
    private List<String> recommendations = Collections.emptyList();

    private Map<String, Object> rated = Collections.emptyMap();
    private Map<String, Object> reviewed = Collections.emptyMap();

    public String getSession() {
        return session;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public long getStart() {
        return start;
    }

    public void setStart(long start) {
        this.start = start;
    }

    public long getEnd() {
        return end;
    }

    public void setEnd(long end) {
        this.end = end;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public List<String> getHover() {
        return hover;
    }

    public void setHover(List<String> hover) {
        this.hover = hover;
    }

    public Map<String, String> getPlayed() {
        return played;
    }

    public void setPlayed(Map<String, String> played) {
        this.played = played;
    }

    public List<String> getActions() {
        return actions;
    }

    public void setActions(List<String> actions) {
        this.actions = actions;
    }

    public List<String> getRecommended() {
        return recommended;
    }

    public void setRecommended(List<String> recommended) {
        this.recommended = recommended;
    }

    public List<String> getRecommendations() {
        return recommendations;
    }

    public void setRecommendations(List<String> recommendations) {
        this.recommendations = recommendations;
    }

    public void setRated(Map<String, Object> rated) {
        this.rated = rated;
    }

    public String getKid() {
        return kid;
    }

    public void setKid(String kid) {
        this.kid = kid;
    }

    public List<String> getBrowsed() {
        return browsed;
    }

    public void setBrowsed(List<String> browsed) {
        this.browsed = browsed;
    }

    public List<String> getPopular() {
        return popular;
    }

    public void setPopular(List<String> popular) {
        this.popular = popular;
    }

    public List<String> getSearched() {
        return searched;
    }

    public void setSearched(List<String> searched) {
        this.searched = searched;
    }

    public List<String> getQueued() {
        return queued;
    }

    public void setQueued(List<String> queued) {
        this.queued = queued;
    }

    public List<String> getRecent() {
        return recent;
    }

    public void setRecent(List<String> recent) {
        this.recent = recent;
    }

    public Map<String, Object> getRated() {
        return rated;
    }

    public Map<String, Object> getReviewed() {
        return reviewed;
    }

    public void setReviewed(Map<String, Object> reviewed) {
        this.reviewed = reviewed;
    }
}
