package com.km.request;

public class GetContentBasedRecommendationRequest {

    private int mid;

    private int num;

    public GetContentBasedRecommendationRequest(int mid, int num) {
        this.mid = mid;
        this.num = num;
    }

    public int getMid() {
        return mid;
    }

    public void setMid(int mid) {
        this.mid = mid;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
