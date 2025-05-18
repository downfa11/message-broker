#include <iostream>
#include <string>
#include <thread>
#include <winsock2.h>
#include <memory>
#include <map>
#include <atomic>
#include <mutex>
#include <random>

#include "topic_manager.h"
#include "buffer_pool.h"
#include "command_handler.h"


#pragma comment(lib, "Ws2_32.lib")

HANDLE hCompletionPort = NULL;
std::atomic<bool> running(true);
std::mutex cout_mutex;


void iocp_worker(HANDLE hCompletionPort) {
    DWORD bytesTransferred;
    ULONG_PTR completionKey;
    LPOVERLAPPED overlapped;
    ClientContext* context;

    while (running) {
        if (!GetQueuedCompletionStatus(hCompletionPort, &bytesTransferred, &completionKey, &overlapped, INFINITE)) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "[error] GetQueuedCompletionStatus: " << GetLastError() << std::endl;
            continue;
        }

        context = reinterpret_cast<ClientContext*>(completionKey);

        if (overlapped == &context->recv_overlapped) {
            if (bytesTransferred == 0) {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cerr << "[" << context->sock << "] connection closed." << std::endl;
                closesocket(context->sock);
                continue;
            }

            std::string receivedData(context->buffer, bytesTransferred);
            std::string response = context->command_handler->handle_command(receivedData, context);

            if (!response.empty()) {
                WSABUF wsabuf;
                wsabuf.buf = const_cast<char*>(response.c_str());
                wsabuf.len = static_cast<ULONG>(response.size());

                DWORD bytesSent = 0;
                DWORD flags = 0;

                ZeroMemory(&context->send_overlapped, sizeof(OVERLAPPED));
                int sendResult = WSASend(context->sock, &wsabuf, 1, &bytesSent, flags, &context->send_overlapped, NULL);

                if (sendResult == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                    std::lock_guard<std::mutex> lock(cout_mutex);
                    std::cerr << "[" << context->sock << " error] WSASend: " << WSAGetLastError() << std::endl;
                    closesocket(context->sock);
                    continue;
                }

                if (response != "NO_MESSAGES") {
                    std::lock_guard<std::mutex> lock(cout_mutex);
                    std::cout << "[" << context->sock << "] sent: " << response << std::endl;
                }
            }

            ZeroMemory(&context->recv_overlapped, sizeof(OVERLAPPED));
            WSABUF wsabuf;
            wsabuf.buf = context->buffer;
            wsabuf.len = sizeof(context->buffer);

            DWORD flags = 0;
            DWORD recvBytes = 0;
            int result = WSARecv(context->sock, &wsabuf, 1, &recvBytes, &flags, &context->recv_overlapped, NULL);
            if (result == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cerr << "[" << context->sock << "] WSARecv failed: " << WSAGetLastError() << std::endl;
                closesocket(context->sock);
            }
        }
        else if (overlapped == &context->send_overlapped) {
            // std::cout << "[" << context->sock << "] send complete." << std::endl;
        }
        else {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "[" << context->sock << "] unknown" << std::endl;
        }
    }
}


bool init_iocp(SOCKET& listenSocket) {
    hCompletionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (hCompletionPort == NULL) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] hCompletionPort: " << GetLastError() << std::endl;
        return false;
    }

    return true;
}

void client_connection_handler(SOCKET clientSocket, BufferPool& bufferPool, const std::string& baseFilename, size_t segmentSize) {
    std::lock_guard<std::mutex> lock(cout_mutex);
    std::cout << "[info] client_connection_handler: " << clientSocket << std::endl;

    ClientContext* context = new ClientContext(bufferPool, std::make_shared<DiskHandler>(baseFilename, segmentSize));
    context->sock = clientSocket;
    context->command_handler = std::make_unique<CommandHandler>(context->disk_handler);

    if (CreateIoCompletionPort((HANDLE)clientSocket, hCompletionPort, (ULONG_PTR)context, 0) == NULL) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] CreateIoCompletionPort: " << GetLastError() << std::endl;
        closesocket(clientSocket);
        delete context;
        return;
    }

    ZeroMemory(&context->recv_overlapped, sizeof(OVERLAPPED));

    WSABUF wsabuf;
    wsabuf.buf = context->buffer;
    wsabuf.len = sizeof(context->buffer);
    DWORD recvBytes = 0;
    DWORD flags = 0;

    int result = WSARecv(clientSocket, &wsabuf, 1, &recvBytes, &flags, &context->recv_overlapped, NULL);
    if (result == SOCKET_ERROR) {
        int error = WSAGetLastError();
        if (error != WSA_IO_PENDING) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "[error] WSARecv: " << error << std::endl;
            closesocket(clientSocket);
            delete context;
            return;
        }
    }
}

std::string random_string(size_t length) {
    static const char charset[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    static thread_local std::mt19937 gen{ std::random_device{}() };
    static thread_local std::uniform_int_distribution<> dist(0, sizeof(charset) - 2);

    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += charset[dist(gen)];
    }
    return result;
}

int main() {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);

    SOCKET listenSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    sockaddr_in service;
    service.sin_family = AF_INET;
    service.sin_addr.s_addr = INADDR_ANY;
    service.sin_port = htons(12345);
    bind(listenSocket, (SOCKADDR*)&service, sizeof(service));
    listen(listenSocket, SOMAXCONN);

    BufferPool bufferPool(10, 1024);

    std::string baseFilename = "broker_log";
    size_t segmentSize = 1024 * 1024;

    if (!init_iocp(listenSocket)) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] init failed IOCP" << std::endl;
        closesocket(listenSocket);
        WSACleanup();
        return 1;
    }

    std::thread iocpThread(iocp_worker, hCompletionPort);
    iocpThread.detach();

    // test topic
    std::thread([]() {
        auto& topicManager = TopicManager::get_instance();
        std::string topic = "topic1";

        while (true) {
            std::string msg = random_string(8);

            topicManager.publish(topic, msg);
            {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "[test] Published to " << topic << " - " << msg << std::endl;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
        }).detach();

        while (true) {
            SOCKET clientSocket = accept(listenSocket, NULL, NULL);
            if (clientSocket != INVALID_SOCKET) {
                client_connection_handler(clientSocket, bufferPool, baseFilename, segmentSize);
            }
        }

        closesocket(listenSocket);
        WSACleanup();
        return 0;
}