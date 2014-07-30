package com.cstor.lock.service;

import com.cstor.JKWatcher;

public interface LockCallback {
	public Object callback(JKWatcher jkWatcher);
}
