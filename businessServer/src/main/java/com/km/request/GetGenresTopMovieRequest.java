package com.km.request;
//获取电影类别的Top电影
public class GetGenresTopMovieRequest {
    private String genres;
    private int num;

    public GetGenresTopMovieRequest(String genres, int num) {
        this.genres = genres;
        this.num = num;
    }

    public String getGenres() {
        return genres;
    }

    public void setGenres(String genres) {
        this.genres = genres;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
