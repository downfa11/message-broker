#ifndef COMMAND_HANDLER_H
#define COMMAND_HANDLER_H

#include <string>
#include "topic.h"
#include "topic_manager.h"

class CommandHandler {
public:
    CommandHandler();

    std::string handle_command(const std::string& cmd, std::string& currentTopic);

private:
    bool is_subscribe_command(const std::string& cmd);
    bool is_pull_command(const std::string& cmd);
};

#endif