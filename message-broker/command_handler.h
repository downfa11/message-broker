#pragma once

#include <string>
#include <memory>
#include <unordered_set>

#include "client_context.h"
#include "disk_handler.h"

class CommandHandler {
public:
    CommandHandler(std::shared_ptr<DiskHandler> disk_handler): disk_handler(std::move(disk_handler)) {}
    std::string handle_command(const std::string& rawCmd, ClientContext* context);

private:
    std::shared_ptr<DiskHandler> disk_handler;
    static bool starts_with(const std::string& str, const std::string& prefix);
    static std::string trim(const std::string& str);
};