package com.demo.project83.common;

import java.util.ArrayList;
import java.util.List;

public class MyFeed {
    List<MyListener> listeners = new ArrayList<>();

    public void register(MyListener listener) {
        listeners.add(listener);
    }

    public void sendMessage(String msg) {
        listeners.forEach(e -> {
            e.priceTick(msg);
        });
    }
}
