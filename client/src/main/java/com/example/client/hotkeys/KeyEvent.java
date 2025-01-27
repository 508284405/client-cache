package com.example.client.hotkeys;

import org.springframework.context.ApplicationEvent;

public class KeyEvent extends ApplicationEvent {
    public KeyEvent(String key) {
        super(key);
    }
}
