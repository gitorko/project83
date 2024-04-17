package com.demo.project83.common;

public interface MyListener {
    void priceTick(String msg);

    void error(Throwable error);
}
