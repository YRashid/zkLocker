package ru.rashid.locker;

public interface ReentrantZKLocker {
    void lock() throws InterruptedException;

    void unlock();
}
