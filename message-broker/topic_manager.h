#ifndef TOPIC_MANAGER_H
#define TOPIC_MANAGER_H

#include <unordered_map>
#include <mutex>
#include <memory>
#include <optional>
#include <sstream>
#include <iostream>
#include "topic.h"
#include "disk_logger.h"

class TopicManager {
public:
    static TopicManager& get_instance() {
        static TopicManager instance;
        return instance;
    }

    void init_logger(const std::string& logPath) {
        std::scoped_lock lock(loggerInitMutex);
        if (!logger) {
            logger = std::make_unique<DiskLogger>(logPath, 1024 * 1024);
        }
    }

    void publish(const std::string& topic, const std::string& msg) {
        std::scoped_lock lock(mtx);
        topicMap[topic].publish(msg);
        if (logger) {
            logger->log("[TopicManager] Published to " + topic + ": " + msg);
        }
    }

    [[nodiscard]]
    std::optional<std::string> pull(const std::string& topic) {
        std::scoped_lock lock(mtx);
        auto it = topicMap.find(topic);
        if (it != topicMap.end()) {
            if (logger) {
                logger->log("[TopicManager] Pulled from topic: " + topic);
            }
            return it->second.pull();
        }
        return std::nullopt;
    }

    [[nodiscard]]
    bool has_topic(const std::string& topic) const {
        std::scoped_lock lock(mtx);
        return topicMap.contains(topic);
    }

    void get_topic_list() const {
        std::ostringstream oss;
        oss << "[TopicManager] Current topics: ";
        for (const auto& [name, _] : topicMap) {
            oss << "'" << name << "' ";
        }
        std::cout << oss.str() << std::endl;
    }

private:
    TopicManager() = default;
    ~TopicManager() = default;
    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;

    mutable std::mutex mtx;
    mutable std::mutex loggerInitMutex;

    std::unordered_map<std::string, TopicQueue> topicMap;
    std::unique_ptr<DiskLogger> logger = nullptr;
};

#endif
