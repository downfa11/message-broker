#pragma once

#include <string>
#include <thread>
#include <winsock2.h>
#include <memory>
#include <unordered_set>

#include "disk_handler.h"

class CommandHandler;
class BufferPool;

struct ClientContext {
    OVERLAPPED recv_overlapped{};
    OVERLAPPED send_overlapped{};

    SOCKET sock;
    BufferPool& pool;
    std::shared_ptr<DiskHandler> disk_handler;
    std::unique_ptr<CommandHandler> command_handler;
    LogCursor cursor;
    std::unordered_set<std::string> currentTopics;
    char buffer[1024];

    ClientContext(BufferPool& p, std::shared_ptr<DiskHandler> d)
        : sock(INVALID_SOCKET), pool(p), disk_handler(std::move(d)), command_handler(nullptr) {
        ZeroMemory(&recv_overlapped, sizeof(recv_overlapped));
        ZeroMemory(&send_overlapped, sizeof(send_overlapped));
    }
};