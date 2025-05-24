#pragma once

#include <unordered_map>
#include <mutex>
#include <memory>
#include <optional>
#include <queue>
#include <string>

#include "disk_handler.h"

class TopicQueue {
private:
    std::mutex mtx;
    std::queue<std::string> q;

public:
    void publish(const std::string& msg);
    std::optional<std::string> pull();
};

class TopicManager {
public:
    static TopicManager& get_instance();

    void init_logger(std::shared_ptr<DiskHandler> diskHandler);
    void publish(const std::string& topic, const std::string& msg);
    [[nodiscard]] std::optional<std::string> pull(const std::string& topic);
    [[nodiscard]] bool has_topic(const std::string& topic) const;
    void get_topic_list() const;

private:
    TopicManager();
    ~TopicManager();
    TopicManager(const TopicManager&) = delete;
    TopicManager& operator=(const TopicManager&) = delete;

    mutable std::mutex mtx;
    mutable std::mutex disk_mutex;

    std::unordered_map<std::string, TopicQueue> topic_map;
    std::shared_ptr<DiskHandler> disk_handler = nullptr;
};