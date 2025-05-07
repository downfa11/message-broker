#pragma once

#include <string>
#include <thread>
#include <winsock2.h>
#include <memory>
#include <unordered_set>

#include "buffer_pool.h"
#include "command_handler.h"
#include "log_cursor.h"
#include "disk_logger.h"


class CommandHandler;

struct ClientContext {
    OVERLAPPED overlapped;
    SOCKET sock;
    BufferPool& pool;
    std::shared_ptr<DiskLogger> diskLogger;
    std::unique_ptr<CommandHandler> commandHandler;
    LogCursor cursor;
    std::unordered_set<std::string> currentTopics;
    char buffer[1024];

    ClientContext(BufferPool& p, std::shared_ptr<DiskLogger> d)
        : sock(INVALID_SOCKET), pool(p), diskLogger(std::move(d)), commandHandler(nullptr) {
        ZeroMemory(&overlapped, sizeof(overlapped));
    }
};