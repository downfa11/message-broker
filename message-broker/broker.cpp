#include <iostream>
#include <string>
#include <thread>
#include <winsock2.h>
#include <memory>
#include <map>
#include <atomic>
#include <mutex>

#include "buffer_pool.h"
#include "command_handler.h"
#include "topic_manager.h"
#include "disk_logger.h"
#include "log_cursor.h"
#include "client_context.h"

#pragma comment(lib, "Ws2_32.lib")

// IOCP
HANDLE hCompletionPort = NULL;
std::atomic<bool> running(true);

void iocp_worker(HANDLE hCompletionPort) {
    DWORD bytesTransferred;
    ULONG_PTR completionKey;
    LPOVERLAPPED overlapped;
    ClientContext* context;

    std::shared_ptr<DiskLogger> logger = std::make_shared<DiskLogger>("logs", 4096);
    CommandHandler commandHandler(logger);

    while (running) {
        if (!GetQueuedCompletionStatus(hCompletionPort, &bytesTransferred, &completionKey, &overlapped, INFINITE)) {
            std::cerr << "GetQueuedCompletionStatus failed. Error: " << GetLastError() << std::endl;
            continue;
        }

        context = reinterpret_cast<ClientContext*>(completionKey);

        if (bytesTransferred > 0) {
            std::string receivedData(context->buffer, bytesTransferred);
            std::cout << "[" << context->sock << "] Received data: " << receivedData << std::endl;

            std::string response = commandHandler.handle_command(receivedData, context);

            WSABUF wsabuf;
            wsabuf.buf = const_cast<char*>(response.c_str());
            wsabuf.len = static_cast<ULONG>(response.size());

            DWORD bytesSent = 0;
            int sendResult = WSASend(context->sock, &wsabuf, 1, &bytesSent, 0, NULL, NULL);
            if (sendResult == SOCKET_ERROR) {
                int error = WSAGetLastError();
                std::cerr << "Send failed. Error: " << error << std::endl;
                break;
            }
            std::cout << "[" << context->sock << "] Response sent: " << response << std::endl;
        }
        else {
            std::cerr << "[" << context->sock << "] Closing socket." << std::endl;
            closesocket(context->sock);
            continue;
        }

        ZeroMemory(&context->buffer, sizeof(context->buffer));

        WSABUF wsabuf;
        wsabuf.buf = context->buffer;
        wsabuf.len = sizeof(context->buffer);
        DWORD recvBytes = 0;
        DWORD recvFlags = 0;

        int result = WSARecv(context->sock, &wsabuf, 1, &recvBytes, &recvFlags, &context->overlapped, NULL);
        if (result == SOCKET_ERROR && WSAGetLastError() != WSA_IO_PENDING) {
            std::cerr << "[" << context->sock << "] WSARecv failed. Error: " << WSAGetLastError() << std::endl;
            closesocket(context->sock);
        }
    }
}

bool init_iocp(SOCKET& listenSocket) {
    hCompletionPort = CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
    if (hCompletionPort == NULL) {
        std::cerr << "Failed to hCompletionPort. Error: " << GetLastError() << std::endl;
        return false;
    }

    return true;
}

void client_connection_handler(SOCKET clientSocket, BufferPool& bufferPool, const std::string& baseFilename, size_t segmentSize) {
    std::cout << "[Server] client_connection_handler: " << clientSocket << std::endl;

    ClientContext* context = new ClientContext(bufferPool, std::make_shared<DiskLogger>(baseFilename, segmentSize));
    context->sock = clientSocket;

    if (CreateIoCompletionPort((HANDLE)clientSocket, hCompletionPort, (ULONG_PTR)context, 0) == NULL) {
        std::cerr << "Failed to associate client. Error: " << GetLastError() << std::endl;
        closesocket(clientSocket);
        delete context;
        return;
    }

    DWORD flags = 0;
    WSABUF wsabuf;
    wsabuf.buf = context->buffer;
    wsabuf.len = sizeof(context->buffer);
    DWORD recvBytes = 0;
    DWORD recvFlags = 0;

    int result = WSARecv(clientSocket, &wsabuf, 1, &recvBytes, &recvFlags, &context->overlapped, NULL);
    if (result == SOCKET_ERROR) {
        int error = WSAGetLastError();
        if (error != WSA_IO_PENDING) {
            std::cerr << "[Server] Error in WSARecv: " << error << std::endl;
            closesocket(clientSocket);
            delete context;
            return;
        }
    }

    std::cout << "[Server] Waiting for data from client..." << std::endl;
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
        std::cerr << "init failed IOCP" << std::endl;
        closesocket(listenSocket);
        WSACleanup();
        return 1;
    }

    std::thread iocpThread(iocp_worker, hCompletionPort);
    iocpThread.detach();

    std::thread([]() {
        int count = 0;
        auto& topicManager = TopicManager::get_instance();

        while (true) {
            std::string topic = "topic1";
            std::string msg = "msg " + std::to_string(count++);

            topicManager.publish(topic, msg);
            std::cout << "[test] Published to topic: " << topic << ": " << msg << std::endl;
            std::this_thread::sleep_for(std::chrono::seconds(2));
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