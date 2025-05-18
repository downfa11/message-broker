#pragma once

#include <vector>
#include <mutex>

class BufferPool {
public:
    BufferPool(size_t count, size_t size) : bufSize(size) {
        for (size_t i = 0; i < count; ++i)
            pool.push_back(new char[size]);
    }

    ~BufferPool() {
        for (auto b : pool) delete[] b;
    }

    char* acquire() {
        std::lock_guard<std::mutex> l(mtx);
        if (pool.empty()) return new char[bufSize];
        char* b = pool.back(); pool.pop_back();
        return b;
    }

    void release(char* b) {
        std::lock_guard<std::mutex> l(mtx);
        pool.push_back(b);
    }

private:
    std::mutex mtx;
    std::vector<char*> pool;
    size_t bufSize;
};