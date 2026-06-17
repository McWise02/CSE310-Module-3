#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <thread>
#include <mutex>
#include <vector>
#include <string>
#include <atomic>
#include <chrono>
#include <iostream>
#include <sstream>
#include <windows.h>
#include <queue>
#include <algorithm>

#pragma comment(lib, "ws2_32.lib")



#define SERVER_PORT 8080
#define MAXLINE 1024
#define BROADCAST_PORT 8888

enum TaskStatus {
    TODO,
    CLAIMED,
    DONE
};

struct Task {
    int id;
    std::string data;
    TaskStatus status;
    std::string claimedBy;
};


class Message
{
public:
    std::string senderIp;
};
class MessageTaskUpdate : public Message
{
    public:
    int taskId;
    TaskStatus newStatus;
    std::string claimedBy;
};


std::vector<std::string> peerIPs;
std::vector<Task> tasks;
std::atomic<bool> running = true;
std::string selfPeer;

std::queue<std::string> broadcastQueue;

std::queue<std::string> outgoingMessages;
std::mutex outgoingMessagesMutex;

//Entering the initial IP address.
//Do this by typing ipconfig in your console and looking for the ipv4 address that is for the WLAN adapter
void EnterIpAddress();
std::string taskStatusToString(TaskStatus status);
TaskStatus stringToTaskStatus(std::string statusText);
void syncTasksToPeer(std::string peerIP);
void startDiscoveryListener();

std::string taskStatusToString(TaskStatus status)
{
    if (status == CLAIMED)
    {
        return "CLAIMED";
    }
    if (status == DONE)
    {
        return "DONE";
    }

    return "TODO";
}

TaskStatus stringToTaskStatus(std::string statusText)
{
    if (statusText == "CLAIMED")
    {
        return CLAIMED;
    }
    if (statusText == "DONE")
    {
        return DONE;
    }

    return TODO;
}

// Sends the full task list to one peer so it can match this computer's list.
void syncTasksToPeer(std::string peerIP)
{
    std::string message = "TASK_LIST|";

    for (int i = 0; i < tasks.size(); i++)
    {
        message += std::to_string(tasks[i].id);
        message += ",";
        message += taskStatusToString(tasks[i].status);
        message += ";";
    }

    SOCKET clientSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (clientSocket == INVALID_SOCKET)
    {
        return;
    }

    sockaddr_in peerAddr{};
    peerAddr.sin_family = AF_INET;
    peerAddr.sin_port = htons(SERVER_PORT);
    inet_pton(AF_INET, peerIP.c_str(), &peerAddr.sin_addr);

    if (connect(clientSocket, (sockaddr*)&peerAddr, sizeof(peerAddr)) != SOCKET_ERROR)
    {
        send(
            clientSocket,
            message.c_str(),
            static_cast<int>(message.size()),
            0
        );
        printf("Sent full task list to %s\n", peerIP.c_str());
    }

    closesocket(clientSocket);
}

// Thread for broadcasting IP Address

void broadcastDiscovery()
{
    SOCKET sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (sock == INVALID_SOCKET)
    {
        printf("Failed to create socket\n");
        return;
    }

    // Allow broadcasting
    BOOL broadcast = TRUE;
    setsockopt(
        sock,
        SOL_SOCKET,
        SO_BROADCAST,
        (char*)&broadcast,
        sizeof(broadcast)
    );

    sockaddr_in broadcastAddr{};
    broadcastAddr.sin_family = AF_INET;
    broadcastAddr.sin_port = htons(8888);
    broadcastAddr.sin_addr.s_addr = INADDR_BROADCAST;

    while (running)
    {
        std::string message = "HELLO|" + selfPeer + "|8888";

        int result = sendto(
            sock,
            message.c_str(),
            static_cast<int>(message.size()),
            0,
            (sockaddr*)&broadcastAddr,
            sizeof(broadcastAddr)
        );

        if (result != SOCKET_ERROR)
        {
            printf("Broadcasted discovery message: %s\n", message.c_str());
        }

        std::this_thread::sleep_for(
            std::chrono::seconds(2)
        );
    }

    closesocket(sock);
}

void EnterIpAddress()
{
    std::string ip;
    std::cout << "Enter your IP Address on WLAN: ";
    std::getline(std::cin, ip);
    selfPeer = ip;
   
}

void startDiscoveryListener()
{
    SOCKET sock = socket(AF_INET, SOCK_DGRAM, IPPROTO_UDP);

    if (sock == INVALID_SOCKET)
    {
        printf("Failed to create discovery listener socket\n");
        return;
    }

    sockaddr_in listenAddr{};
    listenAddr.sin_family = AF_INET;
    listenAddr.sin_port = htons(BROADCAST_PORT);
    listenAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(sock, (sockaddr*)&listenAddr, sizeof(listenAddr)) == SOCKET_ERROR)
    {
        printf("Discovery bind failed: %d\n", WSAGetLastError());
        closesocket(sock);
        return;
    }

    u_long mode = 1;
    ioctlsocket(sock, FIONBIO, &mode);

    while (running)
    {
        char buffer[MAXLINE];
        sockaddr_in senderAddr{};
        int senderLen = sizeof(senderAddr);

        int bytesReceived = recvfrom(
            sock,
            buffer,
            sizeof(buffer) - 1,
            0,
            (sockaddr*)&senderAddr,
            &senderLen
        );

        if (bytesReceived == SOCKET_ERROR)
        {
            Sleep(100);
            continue;
        }

        buffer[bytesReceived] = '\0';
        std::string message(buffer);

        if (message.rfind("HELLO", 0) == 0)
        {
            std::stringstream ss(message);
            std::vector<std::string> messageParts;
            std::string part;

            while (std::getline(ss, part, '|'))
            {
                messageParts.push_back(part);
            }

            if (messageParts.size() >= 2 &&
                std::find(peerIPs.begin(), peerIPs.end(), messageParts[1]) == peerIPs.end() &&
                messageParts[1] != selfPeer)
            {
                std::string newPeerIP = messageParts[1];
                peerIPs.push_back(newPeerIP);
                printf("Discovered peer: %s\n", newPeerIP.c_str());
                syncTasksToPeer(newPeerIP);
            }
        }
    }

    closesocket(sock);
}


// TCP Server for accepting connections and receiving task messages
void startTcpServer()
{
    SOCKET serverSocket =
        socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);

    if (serverSocket == INVALID_SOCKET)
    {
        printf("Failed to create server socket\n");
        return;
    }

    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(SERVER_PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(
        serverSocket,
        (sockaddr*)&serverAddr,
        sizeof(serverAddr)) == SOCKET_ERROR)
    {
        printf("Bind failed: %d\n", WSAGetLastError());
        closesocket(serverSocket);
        return;
    }

    if (listen(serverSocket, SOMAXCONN) == SOCKET_ERROR)
    {
        printf("Listen failed: %d\n", WSAGetLastError());
        closesocket(serverSocket);
        return;
    }

    u_long mode = 1;
    ioctlsocket(serverSocket, FIONBIO, &mode);

    printf("TCP Server listening on port %d\n", SERVER_PORT);

    while (running)
    {
        sockaddr_in clientAddr{};
        int clientLen = sizeof(clientAddr);

        SOCKET clientSocket =
            accept(
                serverSocket,
                (sockaddr*)&clientAddr,
                &clientLen
            );

        if (clientSocket == INVALID_SOCKET)
        {
            Sleep(100);
            continue;
        }

        printf(
            "Client connected from %s\n",
            inet_ntoa(clientAddr.sin_addr)
        );

        std::string connectedPeerIP = inet_ntoa(clientAddr.sin_addr);
        if (std::find(peerIPs.begin(), peerIPs.end(), connectedPeerIP) == peerIPs.end() &&
            connectedPeerIP != selfPeer)
        {
            peerIPs.push_back(connectedPeerIP);
            printf("Added peer from TCP connection: %s\n", connectedPeerIP.c_str());
        }

        char buffer[MAXLINE];

        int bytesReceived =
            recv(
                clientSocket,
                buffer,
                sizeof(buffer) - 1,
                0
            );

        if (bytesReceived > 0)
        {
            buffer[bytesReceived] = '\0';

            printf(
                "Received message: %s\n",
                buffer
            );
            std::string message(buffer);
            std::vector<std::string> messageParts;
            std::stringstream ss(message);
            std::string part;

            while (std::getline(ss, part, '|'))
            {
                messageParts.push_back(part);
            }

            if (message.rfind("TASK_UPDATE", 0) == 0)
            {

                int taskId = std::stoi(messageParts[1]);
                std::string newStatus = messageParts[2];

                for (int i = 0; i < tasks.size(); i++)
                {
                    if (tasks[i].id == taskId)
                    {
                        if (newStatus == "CLAIMED")
                        {
                            tasks[i].status = TaskStatus::CLAIMED;
                            printf("Task %d started by another peer.\n", taskId);
                        }
                        else if (newStatus == "DONE")
                        {
                            tasks[i].status = TaskStatus::DONE;
                            printf("Task %d completed by another peer.\n", taskId);
                        }
                        break;
                    }
                }
                
            }
            if (message.rfind("TASK_LIST", 0) == 0)
            {
                if (messageParts.size() >= 2)
                {
                    std::stringstream taskStream(messageParts[1]);
                    std::string oneTask;

                    while (std::getline(taskStream, oneTask, ';'))
                    {
                        if (oneTask == "")
                        {
                            continue;
                        }

                        std::stringstream oneTaskStream(oneTask);
                        std::string taskIdText;
                        std::string statusText;

                        std::getline(oneTaskStream, taskIdText, ',');
                        std::getline(oneTaskStream, statusText, ',');

                        int taskId = std::stoi(taskIdText);

                        for (int i = 0; i < tasks.size(); i++)
                        {
                            if (tasks[i].id == taskId)
                            {
                                tasks[i].status = stringToTaskStatus(statusText);
                                break;
                            }
                        }
                    }

                    printf("Task list synced from another peer.\n");
                }
            }
        }



        closesocket(clientSocket);
    }

    closesocket(serverSocket);
}


// Thread 3: Worker Thread for processing tasks
//Each task is very simple, and just waits 10 seconds before marking it as complete and sending the message to the other clients in the list. 
void workerLoop()
{
    while (running)
    {
        for (int i = 0; i < tasks.size(); i++)
        {
            Task& newTask = tasks[i];

            if (newTask.status != TaskStatus::TODO)
            {
                i++;
                continue;
            }

            outgoingMessagesMutex.lock();
            outgoingMessages.push(
                "TASK_UPDATE|" + std::to_string(newTask.id) + "|CLAIMED"
            );
            outgoingMessagesMutex.unlock();

            newTask.status = TaskStatus::CLAIMED;
            printf("Task %d started.\n", newTask.id);

            Sleep(1000 * 10);

            outgoingMessagesMutex.lock();
            outgoingMessages.push(
                "TASK_UPDATE|" + std::to_string(newTask.id) + "|DONE"
            );
            outgoingMessagesMutex.unlock();

            newTask.status = TaskStatus::DONE;
            printf("Task %d completed.\n", newTask.id);
        }
    }
}


//Thread 4: Message Thread for sending task updates to other peers
void broadcastMessageThread()
{
    while (running)
    {
        std::string message = "";

        outgoingMessagesMutex.lock();
        if (!outgoingMessages.empty())
        {
            message = outgoingMessages.front();
            outgoingMessages.pop();
        }
        outgoingMessagesMutex.unlock();

        if (message == "")
        {
            Sleep(100);
            continue;
        }

        for (int i = 0; i < peerIPs.size(); i++)
        {
            printf("Trying to send message to %s\n", peerIPs[i].c_str());

            SOCKET clientSocket = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (clientSocket == INVALID_SOCKET)
            {
                printf("Failed to create client socket\n");
                continue;
            }

            sockaddr_in peerAddr{};
            peerAddr.sin_family = AF_INET;
            peerAddr.sin_port = htons(SERVER_PORT);
            inet_pton(AF_INET, peerIPs[i].c_str(), &peerAddr.sin_addr);

            if (connect(clientSocket, (sockaddr*)&peerAddr, sizeof(peerAddr)) != SOCKET_ERROR)
            {
                int sendResult = send(
                    clientSocket,
                    message.c_str(),
                    static_cast<int>(message.size()),
                    0
                );

                if (sendResult == SOCKET_ERROR)
                {
                    printf("Send failed to %s: %d\n", peerIPs[i].c_str(), WSAGetLastError());
                }
                else
                {
                    printf("Sent message to %s: %s\n", peerIPs[i].c_str(), message.c_str());
                }
            }
            else
            {
                printf("Connect failed to %s: %d\n", peerIPs[i].c_str(), WSAGetLastError());
            }

            closesocket(clientSocket);
        }
    }
}
//Fake Task LIst initialization
void initTasksList()
{
    for (int i = 0; i < 20; ++i)
    {
        Task task;
        task.id = i + 1;
        task.data = "Task data " + std::to_string(i + 1);
        task.status = TODO;
        tasks.push_back(task);
    }
}


int main() {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2,2), &wsaData);
    initTasksList();
    EnterIpAddress();
    std::thread broadcaster(broadcastDiscovery);
    std::thread discoveryListener(startDiscoveryListener);
    std::thread tcpServer(startTcpServer);
    std::thread worker(workerLoop);
    std::thread messageThread(broadcastMessageThread);


    std::cin.get();

    running = false;
    broadcaster.join();
    discoveryListener.join();
    tcpServer.join();
    worker.join();
    messageThread.join();

    WSACleanup();
    return 0;
}

//example

// int main() {
//     WSADATA wsaData;
//     WSAStartup(MAKEWORD(2, 2), &wsaData);

//     SOCKET sockfd;
//     char buffer[MAXLINE];
//     const char *hello = "Hello from client";
//     struct sockaddr_in servaddr;

//     sockfd = socket(AF_INET, SOCK_DGRAM, 0);
//     if (sockfd == INVALID_SOCKET) {
//         printf("socket creation failed: %d\n", WSAGetLastError());
//         WSACleanup();
//         return 1;
//     }

//     memset(&servaddr, 0, sizeof(servaddr));

//     servaddr.sin_family = AF_INET;
//     servaddr.sin_port = htons(PORT);
//     servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");

//     int len = sizeof(servaddr);

//     sendto(sockfd, hello, strlen(hello), 0,
//            (const struct sockaddr *)&servaddr, sizeof(servaddr));

//     printf("Hello message sent.\n");

//     int n = recvfrom(sockfd, buffer, MAXLINE - 1, 0,
//                      (struct sockaddr *)&servaddr, &len);

//     if (n == SOCKET_ERROR) {
//         printf("recvfrom failed: %d\n", WSAGetLastError());
//     } else {
//         buffer[n] = '\0';
//         printf("Server: %s\n", buffer);
//     }

//     closesocket(sockfd);
//     WSACleanup();

//     return 0;
// }
