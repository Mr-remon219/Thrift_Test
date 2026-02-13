#include <iostream>
#include "./match-server/Match.h"
#include "./save-client/Save.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/TToString.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TSocket.h>

#include <mutex>
#include <condition_variable>
#include <queue>
#include <vector>
#include <unistd.h>
#include <cstdlib>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using namespace ::match_service;
using namespace ::save_service;


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
            printf("match result: %d %d\n", user1.id, user2.id);
            std::shared_ptr<TTransport> socket(new TSocket("123.57.67.128", 9090));
            std::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
            std::shared_ptr<TProtocol> protocol(new TBinaryProtocol(transport));
            SaveClient client(protocol);

            try {
                transport->open();
                uint32_t res = client.save_data("acs_13185", "3f05c09f", user1.id, user2.id);
                if ( !res ) std::puts("success");
                else std::puts("failed");
                transport->close();
            } catch (TException& tx) {
                std::cout << "ERROR: " << tx.what() << '\n';
            }
        }

        bool check_match(uint32_t a, uint32_t b)
        {
            uint32_t res = std::abs( users[a].score - users[b].score );
            uint32_t a_diff = 50 * wt[a];
            uint32_t b_diff = 50 * wt[b];

            return res <= a_diff && res <= b_diff;
        }

        void match()
        {
            for ( uint32_t i = 0; i < users.size(); i ++ )
            {
                wt[i] ++;
            }
            if ( users.size() > 1 )
            {
                bool flag = false;
                for ( uint32_t i = 0; i < users.size(); i ++ )
                {
                    for ( uint32_t j = i + 1; j < users.size(); j ++ )
                    {
                        if ( check_match(i, j) )
                        {
                            User user1 = users[i];
                            User user2 = users[j];
                            users.erase(users.begin() + i);
                            wt.erase(wt.begin() + i);
                            users.erase(users.begin() + j - 1);
                            wt.erase(wt.begin() + j - 1);
                            flag = true;
                            save_data(user1, user2);
                            break;
                        }
                    }
                    if ( flag )
                        break;
                }

            }
        }

        void add(User user)
        {
            users.push_back(user);
            wt.push_back(0);
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
        std::vector<uint32_t> wt;
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

class MatchCloneFactory : virtual public MatchIfFactory {
 public:
  ~MatchCloneFactory() override = default;
  MatchIf* getHandler(const ::apache::thrift::TConnectionInfo& connInfo) override
  {
    std::shared_ptr<TSocket> sock = std::dynamic_pointer_cast<TSocket>(connInfo.transport);
    std::cout << "Incoming connection\n";
    std::cout << "\tSocketInfo: "  << sock->getSocketInfo() << "\n";
    std::cout << "\tPeerHost: "    << sock->getPeerHost() << "\n";
    std::cout << "\tPeerAddress: " << sock->getPeerAddress() << "\n";
    std::cout << "\tPeerPort: "    << sock->getPeerPort() << "\n";
    return new MatchHandler;
  }
  void releaseHandler(MatchIf* handler) override {
    delete handler;
  }
};

void consumer()
{
    while (true)
    {
        std::unique_lock<std::mutex> lck(message_queue.m);
        if (message_queue.q.empty())
        {
            lck.unlock();
            pool.match();
            sleep(1);
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
        }
    }
}

int main(int argc, char **argv) {
  TThreadedServer server(
    std::make_shared<MatchProcessorFactory>(std::make_shared<MatchCloneFactory>()),
    std::make_shared<TServerSocket>(9090), //port
    std::make_shared<TBufferedTransportFactory>(),
    std::make_shared<TBinaryProtocolFactory>());


    std::thread matching_thread(consumer);
    std::cout << "server start!" << std::endl;
    server.serve();
    return 0;
}

