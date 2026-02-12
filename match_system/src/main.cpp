#include <iostream>
#include "./match-server/Match.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace  ::match_service;


struct Task
{
    User user;
    std::string type;
};

struct MessageQueue
{
    std::queue<Task> q;
    std::mutex m;
    std::condition_variable cv;
}message_queue;


class Pool
{
    public:
        void save_data(User user1, User user2)
        {
            std::cout << "save, success!" << std::endl;
        }
        
        void match()
        {
            if ( users.size() > 1 )
            {
                User user1 = users[0];
                User user2 = users[1];
                users.erase(users.begin());
                users.erase(users.begin());
                std::cout << "match, success" << user1.id << " " << user2.id << std::endl;
                save_data(user1, user2);
            }
        }

        void add(User user)
        {
            users.push_back(user);
            std::cout << "add, success" << std::endl;
        }

        void remove(User user)
        {
            for ( uint32_t i = 0; i < users.size(); i ++ )
                if ( user.id == users[i].id )
                {
                    users.erase(users.begin() + i);
                    std::cout << "remove, success"<< std::endl;
                    break;
                }
        }



    private:
        std::vector<User> users;
}pool;

class MatchHandler : virtual public MatchIf {
    public:
        MatchHandler() {
            // Your initialization goes here
        }

        /**
         * user: 添加的用户信息
         * info: 附加信息
         * 在匹配池中添加一个名用户
         * 
         * @param user
         * @param info
         */
        int32_t add_user(const User& user, const std::string& info) {
            // Your implementation goes here
            printf("add_user\n");
            std::unique_lock<std::mutex> lck(message_queue.m);
            message_queue.q.push({user, "add"});
            lck.unlock();
            message_queue.cv.notify_all();
            return 0;
        }

        /**
         * user: 删除的用户信息
         * info: 附加信息
         * 从匹配池中删除一名用户
         * 
         * @param user
         * @param info
         */
        int32_t remove_user(const User& user, const std::string& info) {
            // Your implementation goes here
            printf("remove_user\n");
            std::unique_lock<std::mutex> lck(message_queue.m);
            message_queue.q.push({user, "remove"});
            lck.unlock();
            message_queue.cv.notify_all();
            return 0;
        }

};

void consumer()
{
    while (true)
    {
        std::unique_lock<std::mutex> lck(message_queue.m);
        if (message_queue.q.empty())
        {
            message_queue.cv.wait(lck);
        }
        else
        {
            User user = message_queue.q.front().user;
            std::string type = message_queue.q.front().type;
            message_queue.q.pop();
            if ( type == "add" )
                pool.add(user);
            else if ( type == "remove" )
                pool.remove(user);
            lck.unlock();
            pool.match();
        }
    }
}

int main(int argc, char **argv) {
    int port = 9090;
    ::std::shared_ptr<MatchHandler> handler(new MatchHandler());
    ::std::shared_ptr<TProcessor> processor(new MatchProcessor(handler));
    ::std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    ::std::shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
    ::std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

    TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
    std::thread matching_thread(consumer);
    std::cout << "server start!" << std::endl;
    server.serve();
    return 0;
}

