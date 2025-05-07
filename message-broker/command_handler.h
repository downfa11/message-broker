#ifndef COMMAND_HANDLER_H
#define COMMAND_HANDLER_H

#include <string>
#include <memory>
#include <unordered_set>

#include "topic.h"
#include "topic_manager.h"
#include "disk_logger.h"
#include "client_context.h"

class CommandHandler {
public:
    CommandHandler(std::shared_ptr<DiskLogger> logger)
        : diskLogger(std::move(logger)) {}

    std::string handle_command(const std::string& rawCmd, ClientContext* context) {
        std::string cmd = trim(rawCmd);
        diskLogger->log("Received command: " + cmd);

        if (is_subscribe_command(cmd)) {
            std::string newTopic = cmd.substr(10);
            context->currentTopics.insert(newTopic);
            diskLogger->log("[CommandHandler] Subscribed to topic: " + newTopic);
            return "OK\n";
        }
        else if (is_pull_command(cmd)) {
            if (context->currentTopics.empty()) {
                diskLogger->log("[CommandHandler] No topic subscribed yet.");
                return "NO_TOPIC\n";
            }

            std::string response = "NO_MESSAGES_FOR_TOPIC\n";
            for (const auto& topic : context->currentTopics) {
                if (TopicManager::get_instance().has_topic(topic)) {
                    auto msg = TopicManager::get_instance().pull(topic);
                    if (msg) {
                        diskLogger->log("[CommandHandler] Pulled message from topic: " + topic);
                        response = "OK: " + msg.value();
                        break;
                    }
                    else {
                        diskLogger->log("[CommandHandler] Topic " + topic + " is empty.");
                    }
                }
                else {
                    diskLogger->log("[CommandHandler] No such topic: " + topic);
                    response = "NO_TOPIC\n";
                }
            }
            return response;
        }
        else {
            diskLogger->log("[CommandHandler] Invalid command: " + cmd);
            return "INVALID_CMD: " + cmd + "\n";
        }
    }

private:
    std::shared_ptr<DiskLogger> diskLogger;

    bool is_subscribe_command(const std::string& cmd) {
        return cmd.substr(0, 10) == "SUBSCRIBE ";
    }

    bool is_pull_command(const std::string& cmd) {
        return cmd.substr(0, 4) == "PULL";
    }

    static std::string trim(const std::string& str) {
        size_t start = str.find_first_not_of(" \r\n\t");
        size_t end = str.find_last_not_of(" \r\n\t");
        return (start == std::string::npos) ? "" : str.substr(start, end - start + 1);
    }
};

#endif