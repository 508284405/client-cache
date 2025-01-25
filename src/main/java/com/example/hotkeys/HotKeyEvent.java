package com.example.hotkeys;

import org.springframework.context.ApplicationEvent;

public class HotKeyEvent extends ApplicationEvent {
    public HotKeyEvent(String key) {
        super(key);
    }
}
