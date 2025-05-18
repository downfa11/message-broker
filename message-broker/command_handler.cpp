#include "command_handler.h"
#include "topic_manager.h"

#include <algorithm>

std::string CommandHandler::handle_command(const std::string& rawCmd, ClientContext* context) {
    std::string cmd = trim(rawCmd);
    disk_handler->log("received command: " + cmd);

    if (starts_with(cmd, "SUBSCRIBE ")) {
        std::string newTopic = cmd.substr(10);
        context->currentTopics.insert(newTopic);
        disk_handler->log("[info] Subscribed to topic: " + newTopic);
        return "OK";
    }

    if (starts_with(cmd, "PULL")) {
        if (context->currentTopics.empty()) {
            disk_handler->log("[info] No topic subscribed yet.");
            return "NO_TOPIC";
        }

        for (const auto& topic : context->currentTopics) {
            if (!TopicManager::get_instance().has_topic(topic)) {
                disk_handler->log("[info] No such topic: " + topic);
                continue;
            }

            auto msg = TopicManager::get_instance().pull(topic);
            if (msg) {
                disk_handler->log("[info] Pulled message from topic: " + topic);
                return msg.value();
            }
            else {
                disk_handler->log("[info] Topic " + topic + " is empty.");
            }
        }

        return "NO_MESSAGES";
    }

    if (starts_with(cmd, "PUBLISH ")) { // PUBLISH <topic> <message>
        size_t firstSpace = cmd.find(' ', 8);
        if (firstSpace == std::string::npos) {
            disk_handler->log("[info] Invalid PUBLISH command format.");
            return "INVALID_CMD: " + cmd;
        }
        std::string topic = cmd.substr(8, firstSpace - 8);
        std::string message = cmd.substr(firstSpace + 1);
        TopicManager::get_instance().publish(topic, message);
        disk_handler->log("[info] Published message to topic: " + topic);
        return "OK";
    }

    disk_handler->log("[info] Invalid command: " + cmd);
    return "INVALID_CMD: " + cmd;
}

bool CommandHandler::starts_with(const std::string& str, const std::string& prefix) {
    return str.size() >= prefix.size() && std::equal(prefix.begin(), prefix.end(), str.begin());
}

std::string CommandHandler::trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \r\n\t");
    size_t end = str.find_last_not_of(" \r\n\t");
    return (start == std::string::npos) ? "" : str.substr(start, end - start + 1);
}
