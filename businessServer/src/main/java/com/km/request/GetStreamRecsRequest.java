package com.km.request;

public class GetStreamRecsRequest {

    private int uid;

    private int num;

    public GetStreamRecsRequest(int uid, int num) {
        this.uid = uid;
        this.num = num;
    }

    public int getUid() {
        return uid;
    }

    public void setUid(int uid) {
        this.uid = uid;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }
}
