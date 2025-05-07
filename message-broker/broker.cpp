#include <iostream>
#include <string>
#include <thread>
#include <winsock2.h>

#include "buffer_pool.h"
#include "command_handler.h"
#include "topic_manager.h"

#pragma comment(lib, "Ws2_32.lib")

void client_handler(SOCKET sock, BufferPool& pool) {
    std::string currentTopic;
    CommandHandler commandHandler;

    u_long mode = 1; // 1 for non-blocking
    ioctlsocket(sock, FIONBIO, &mode);

    char* buf = pool.acquire();

    while (true) {
        WSAOVERLAPPED overlapped = { 0 };
        WSABUF wsabuf;
        wsabuf.len = 1024;
        wsabuf.buf = buf;

        DWORD bytesReceived = 0;
        DWORD flags = 0;

        int result = WSARecv(sock, &wsabuf, 1, &bytesReceived, &flags, &overlapped, NULL);


        if (result == SOCKET_ERROR) {
            if (WSAGetLastError() == WSAEWOULDBLOCK) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            }
            else {
                std::cerr << "Recv failed. Error: " << WSAGetLastError() << std::endl;
                break;
            }
        }

        if (bytesReceived == 0) {
            break;
        }

        std::string cmd(buf, bytesReceived);


        size_t pos = 0;
        while ((pos = cmd.find("\n")) != std::string::npos) {
            std::string command = cmd.substr(0, pos);
            cmd.erase(0, pos + 1);

            std::string response = commandHandler.handle_command(command, currentTopic);
            std::cout << "[" << socket << "] " << command << " : " << response << std::endl;


            WSABUF buffer;
            buffer.len = response.size();
            buffer.buf = const_cast<char*>(response.c_str());

            DWORD bytesSent = 0;
            int result = WSASend(sock, &buffer, 1, &bytesSent, 0, NULL, NULL);

            if (result == SOCKET_ERROR) {
                int error = WSAGetLastError();
                if (error != WSAEWOULDBLOCK) {
                    std::cerr << "Send failed. Error: " << error << std::endl;
                    break;
                }
            }
            else {
                std::cout << "Data sent successfully" << std::endl;
            }
        }
        pool.release(buf);
    }
    closesocket(sock);
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
            std::thread(client_handler, clientSocket, std::ref(bufferPool)).detach();
        }
    }

    closesocket(listenSocket);
    WSACleanup();
    return 0;
}