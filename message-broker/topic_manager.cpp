#include "topic_manager.h"

#include <sstream>
#include <iostream>


void TopicQueue::publish(const std::string& msg) {
    std::lock_guard<std::mutex> lock(mtx);
    q.push(msg);
}

std::optional<std::string> TopicQueue::pull() {
    std::lock_guard<std::mutex> lock(mtx);
    if (q.empty()) return std::nullopt;
    std::string m = q.front(); q.pop();
    return m;
}


TopicManager::TopicManager() = default;
TopicManager::~TopicManager() = default;

TopicManager& TopicManager::get_instance() {
    static TopicManager instance;
    return instance;
}

void TopicManager::init_logger(std::shared_ptr<DiskHandler> diskHandler) {
    std::scoped_lock lock(disk_mutex);
    disk_handler = std::move(diskHandler);
}

void TopicManager::publish(const std::string& topic, const std::string& msg) {
    std::scoped_lock lock(mtx);
    topic_map[topic].publish(msg);
    disk_handler->log("info", "Published to " + topic + ": " + msg);
}

std::optional<std::string> TopicManager::pull(const std::string& topic) {
    std::scoped_lock lock(mtx);
    auto it = topic_map.find(topic);
    if (it != topic_map.end()) {
        disk_handler->log("info", "Pulled from topic: " + topic);
        return it->second.pull();
    }
    return std::nullopt;
}

bool TopicManager::has_topic(const std::string& topic) const {
    std::scoped_lock lock(mtx);
    return topic_map.contains(topic);
}

void TopicManager::get_topic_list() const {
    std::scoped_lock lock(mtx);
    std::ostringstream oss;
    oss << "[info] Current topics: ";
    for (const auto& [name, _] : topic_map) {
        oss << "'" << name << "' ";
    }
    std::cout << oss.str() << std::endl;
}
