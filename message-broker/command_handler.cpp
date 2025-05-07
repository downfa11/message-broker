#include "command_handler.h"
#include "topic_manager.h"

#include <string>
#include <iostream>

static std::string trim(const std::string& str) {
    size_t start = str.find_first_not_of(" \r\n\t");
    size_t end = str.find_last_not_of(" \r\n\t");
    return (start == std::string::npos) ? "" : str.substr(start, end - start + 1);
}

CommandHandler::CommandHandler() {}

std::string CommandHandler::handle_command(const std::string& rawCmd, std::string& currentTopic) {
    std::string cmd = trim(rawCmd);

    if (is_subscribe_command(cmd)) {
        currentTopic = cmd.substr(10);
        std::cout << "[CommandHandler] Subscribed to topic: " << currentTopic << std::endl;
        return "OK\n";
    }
    else if (is_pull_command(cmd)) {
        if (TopicManager::get_instance().has_topic(currentTopic)) {
            auto msg = TopicManager::get_instance().pull(currentTopic);
            if (msg) {
                return "OK: " + msg.value();
            }
            else {
                std::cout << "[CommandHandler] Topic is empty: " << currentTopic << std::endl;
                return "EMPTY\n";
            }
        }
        else {
            return "NO_TOPIC\n";
        }
    }
    else {
        return "INVALID_CMD\n";
    }
}

bool CommandHandler::is_subscribe_command(const std::string& cmd) {
    return cmd.substr(0, 10) == "SUBSCRIBE ";
}

bool CommandHandler::is_pull_command(const std::string& cmd) {
    return cmd.substr(0, 4) == "PULL";
}

