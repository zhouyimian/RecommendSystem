package com.km.request;

import java.util.ArrayList;
import java.util.List;

public class UpdateUserGenresRequest {

    private String username;

    private List<String> genres = new ArrayList<>();

    public UpdateUserGenresRequest(String username, List<String> genres) {
        this.username = username;
        this.genres = genres;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public List<String> getGenres() {
        return genres;
    }

    public void setGenres(List<String> genres) {
        this.genres = genres;
    }
}
