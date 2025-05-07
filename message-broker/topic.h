#ifndef TOPIC_QUEUE_H
#define TOPIC_QUEUE_H

#include <queue>
#include <string>
#include <mutex>
#include <optional>

class TopicQueue {
private:
    std::mutex mtx;
    std::queue<std::string> q;

public:
    void publish(const std::string& msg) {
        std::lock_guard<std::mutex> lock(mtx);
        q.push(msg);
    }

    std::optional<std::string> pull() {
        std::lock_guard<std::mutex> lock(mtx);
        if (q.empty()) return std::nullopt;
        std::string m = q.front(); q.pop();
        return m;
    }
};

#endif
