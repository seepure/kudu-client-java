package org.seepure.kudu.client.vo;

public class KuduOpResult {
    private int code;
    private String msg;

    public KuduOpResult() {
    }

    public KuduOpResult(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
