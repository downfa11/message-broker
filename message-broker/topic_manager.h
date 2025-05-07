#ifndef TOPIC_MANAGER_H
#define TOPIC_MANAGER_H

#include <unordered_map>
#include <mutex>
#include <iostream>

#include "topic.h"

class TopicManager {
public:
    static TopicManager& get_instance() {
        static TopicManager instance;
        return instance;
    }

    void publish(const std::string& topic, const std::string& msg) {
        std::lock_guard<std::mutex> lock(mtx);
        topicMap[topic].publish(msg);
    }

    std::optional<std::string> pull(const std::string& topic) {
        std::lock_guard<std::mutex> lock(mtx);
        if (topicMap.find(topic) != topicMap.end()) {
            std::cout << "[TopicManager] Topic found: " << topic << std::endl;
            return topicMap[topic].pull();
        }
        std::cout << "[TopicManager] No such topic: " << topic << std::endl;
        return std::nullopt;
    }

    bool has_topic(const std::string& topic) {
        std::lock_guard<std::mutex> lock(mtx);
        return topicMap.find(topic) != topicMap.end();
    }

    void get_topic_list() {
        std::cout << "[TopicManager] Current topics in map: ";
        for (const auto& pair : topicMap) {
            std::cout << "'" << pair.first << "' ";
        }
        std::cout << std::endl;
    }

private:
    TopicManager() = default;
    ~TopicManager() = default;
    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;

    std::unordered_map<std::string, TopicQueue> topicMap;
    std::mutex mtx;
};

#endif