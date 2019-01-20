package com.km.request;
//获取电影类别的Top电影
public class GetRateMoreMoviesRequest {

    private int num;

    public GetRateMoreMoviesRequest(int num) {
        this.num = num;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
