#include <unistd.h>
#include <gtest/gtest.h>
#include "lib.h"

Poco::Data::Session* ConnectorSQL() {
    std::string keyConnect = "host=" + NConnect::host + ";user=" + NConnect::log + ";db=" + NConnect::db + ";password=" 
                           + NConnect::pass + ";port=" + std::to_string(NConnect::portDb);//
    Poco::Data::MySQL::Connector::registerConnector();
    Poco::Data::Session* session = nullptr;
    try {                                                       
        session = new Poco::Data::Session(Poco::Data::SessionFactory::instance().create(Poco::Data::MySQL::Connector::KEY,
                                                                                        keyConnect));
    }               
    catch (Poco::Data::MySQL::ConnectionException& e) {             
        std::cout << "connection ERROR:" << e.what() << std::endl;  
    }                                                               
    catch (Poco::Data::MySQL::StatementException& e) {              
        std::cout << "statement ERROR:" << e.what() << std::endl;   
    }
    return session;
}

void ThreadCreateTable(int idShard) {
    auto sqlSession = std::unique_ptr<Poco::Data::Session>(ConnectorSQL());
    auto& session = *sqlSession;
    try {                                                           
        Poco::Data::Statement checkTable(session);
        Poco::Data::Statement requestSQL(session);
        checkTable << "DROP TABLE IF EXISTS Person -- sharding:" + std::to_string(idShard);
        requestSQL << "CREATE TABLE IF NOT EXISTS Person ("
                   << "    login VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL PRIMARY KEY,"
                   << "    first_name VARCHAR(15) CHARACTER SET utf8 COLLATE utf8_unicode_ci  NULL,"
                   << "    last_name VARCHAR(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci  NOT NULL,"
                   << "    age INT NULL CHECK(age >= 0)"
                   << ") -- sharding:" << std::to_string(idShard);
        checkTable.execute();
        requestSQL.execute();
    }                                                               
    catch (Poco::Data::MySQL::ConnectionException& e) {             
        std::cout << "connection ERROR:" << e.what() << std::endl;  
    }                                                               
    catch (Poco::Data::MySQL::StatementException& e) {              
        std::cout << "statement ERROR:" << e.what() << std::endl;   
    }
}

void ThreadAdd(Person testUser) {
    Poco::Net::SocketAddress addr(NConnect::ip, NConnect::port);
    Poco::Net::StreamSocket socket(addr);
    Poco::Net::SocketStream requestPost(socket);
    requestPost << "POST /person HTTP/1.1\ncontent-type: application/url-encoded\n\nlogin=" << testUser.login << "&first_name=" 
                << testUser.first_name << "&last_name=" << testUser.last_name << "&age=" << testUser.age << "\n";
    requestPost.flush();
    socket.shutdownSend();
}

void check_person(Person testUser, int* index) {
    *index = 1;
    Poco::Net::SocketAddress addr(NConnect::ip, NConnect::port);
    Poco::Net::StreamSocket socket(addr);
    Poco::Net::SocketStream requestGet(socket);
    requestGet << "GET /person?login=" << testUser.login << "\nHTTP/1.1\n";
    requestGet.flush();
    socket.shutdownSend();
    std::stringstream jsonString;
    Poco::StreamCopier::copyStream(requestGet, jsonString);
    std::vector<char> tmpSting(256);
    std::string resultString;
    while (jsonString) {
        jsonString.getline(tmpSting.data(), tmpSting.size(), '\n');
        resultString += tmpSting.data();
    }
    std::string age = std::to_string(testUser.age);
    if (std::search(resultString.begin(), resultString.end(), testUser.login.begin(), testUser.login.end()) == resultString.end() 
     || std::search(resultString.begin(), resultString.end(), testUser.first_name.begin(), testUser.first_name.end()) == resultString.end() 
     || std::search(resultString.begin(), resultString.end(), testUser.last_name.begin(), testUser.last_name.end()) == resultString.end() 
     || std::search(resultString.begin(), resultString.end(), age.begin(), age.end()) == resultString.end()) {
        *index = 0;
    }
}

TEST(clearDBTest, myTest) {// 
    testing::internal::CaptureStdout();
    std::vector<std::thread*> threadVector(NConnect::shardCount);
    for (int i = 0; i < NConnect::shardCount; i++)
        threadVector[i] = new std::thread(ThreadCreateTable, i);
    for (int i = 0; i < NConnect::shardCount; i++) {
        threadVector[i]->join(); 
        delete threadVector[i];
    }
    ASSERT_TRUE(testing::internal::GetCapturedStdout() == "");
}

TEST(firstTest, myTest) {//
    testing::internal::CaptureStdout();
    std::vector<Person> testUsers = {{"SamST", "Samuel",     "Stone",    32},
                                     {"Globe", "Globus",     "Kvadratov", 1},
                                     {"VIP",   "Vyacheslav", "Alehin",    7},
                                     {"KAR",   "Roman",      "Kazakov",  65},
                                     {"PePe",  "Petr",       "Petrov",   23},
                                     {"Navi",  "Ivan",       "Ivanov",   15},};
    int size = testUsers.size();
    std::vector<std::thread*> threadVector(size);
    for (int i = 0; i < size; i++) // параллельно создаём запросы на добавление
        threadVector[i] = new std::thread(ThreadAdd, testUsers[i]);
    for (int i = 0; i < size; i++) {
        threadVector[i]->join(); 
        delete threadVector[i];
    }
    sleep(5); // ждём на всякий случай
    std::vector<int> result(size); // результаты запросов
    for (int i = 0; i < size; i++) // параллельно создаём запросы на поиск
        threadVector[i] = new std::thread(check_person, testUsers[i], result.data() + i);
    for (int i = 0; i < size; i++) {
        threadVector[i]->join(); 
        delete threadVector[i];
    }
    ASSERT_TRUE(testing::internal::GetCapturedStdout() == ""); // проверяем ошибки
    for (int i = 0; i < size; i++)
        ASSERT_TRUE(result[i]);
}

int main(int argc, char *argv[]) {//
    if (argc != 2) {
        std::cout << "ERROR: no ip input" << std::endl;
        return 0;
    }
    std::map<std::string, std::string> argMap;
    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);
        int j = std::find(arg.begin(), arg.end(), '=') - arg.begin();
        if (j == arg.size()) {
            std::cout << "ERROR: wrong argc[" + std::to_string(i) + "]" << std::endl;
            return 0;
        }
        argMap[arg.substr(0, j)] = arg.substr(j + 1, arg.size() - j);
    }
    if (argMap.find("--ip") == argMap.end()) {
        std::cout << "ERROR: no ip in argc" << std::endl;
        return 0;
    }
    NConnect::ip = argMap["--ip"];



    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
