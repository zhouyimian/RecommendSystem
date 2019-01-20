package com.km.request;

public class GetItemCFMoviesRequest {
    private int mid;

    private int num;

    public GetItemCFMoviesRequest(int mid, int num) {
        this.mid = mid;
        this.num = num;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }
}
