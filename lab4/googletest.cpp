#include <unistd.h>
#include <gtest/gtest.h>
#include "lib.h"

Poco::Data::Session* ConnectorSQL() {
    std::string keyConnect = "host=" + NConnect::host + ";user=" + NConnect::log + ";db=" + NConnect::db 
                           + ";password=" +  NConnect::pass + ";port=" + std::to_string(NConnect::dbPort);
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

void Add(Person testUser) {
    Poco::Net::SocketAddress addr(NConnect::ip, NConnect::port);
    Poco::Net::StreamSocket socket(addr);
    Poco::Net::SocketStream requestPost(socket);
    requestPost << "POST /person HTTP/1.1\ncontent-type: application/url-encoded\n\nlogin=" << testUser.login << "&first_name=" 
                << testUser.first_name << "&last_name=" << testUser.last_name << "&age=" << testUser.age << "\n";
    requestPost.flush();
    socket.shutdownSend();
}

bool Search(Person testUser) {
    Poco::Net::SocketAddress addr(NConnect::ip, NConnect::port);
    Poco::Net::StreamSocket socket(addr);
    Poco::Net::SocketStream requestPost(socket);
    requestPost << "GET /person?login=" << testUser.login << "\nHTTP/1.1\n";
    requestPost.flush();
    socket.shutdownSend();
    std::stringstream jsonString;
    Poco::StreamCopier::copyStream(requestPost, jsonString);
    std::string strJson = jsonString.str();
    int start = strJson.find('{');
    std::string jsonResult = strJson.substr(start, strJson.find('}') - start + 1);
    Poco::JSON::Parser jsonParser;
    bool found = true;
    try {
        Poco::Dynamic::Var parsed = jsonParser.parse(jsonResult);
        Poco::JSON::Object::Ptr object = parsed.extract<Poco::JSON::Object::Ptr>();
        std::string login = object->get("login").toString();
        std::string firstName = object->get("first_name").toString();
        std::string lastName = object->get("last_name").toString();
        int age;
        object->get("age").convert(age);
        found = (login == testUser.login) && (firstName == testUser.first_name) && (lastName == testUser.last_name) 
                && (age == testUser.age);
    }
    catch(...) {
        found = false;
    }
    return found;
}

std::string GetSingInData(std::map<std::string, std::string> argMap, std::string data) {
    if (argMap.find("--" + data) == argMap.end()) {
        std::cout << "ERROR: no " << data << " in arg" << std::endl;
        return "-1";
    }
    return argMap["--" + data];
}

TEST(clearDBTest, myTest) {
    testing::internal::CaptureStdout();
    auto sqlSession = std::unique_ptr<Poco::Data::Session>(ConnectorSQL());
    auto& session = *sqlSession;
    try {
        Poco::Data::Statement checkTable(session);
        Poco::Data::Statement requestSQL(session);
        checkTable << "DROP TABLE IF EXISTS Person"; 
        requestSQL << "CREATE TABLE IF NOT EXISTS Person ("
                   << "    login VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL PRIMARY KEY,"
                   << "    first_name VARCHAR(15) CHARACTER SET utf8 COLLATE utf8_unicode_ci  NULL,"
                   << "    last_name VARCHAR(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci  NOT NULL,"
                   << "    age INT NULL CHECK(age >= 0)"
                   << ")";
        checkTable.execute();
        requestSQL.execute();
    }
    catch (Poco::Data::MySQL::ConnectionException& e) {
        std::cout << "connection ERROR:" << e.what() << std::endl;
    }
    catch (Poco::Data::MySQL::StatementException& e) {
        std::cout << "statement ERROR:" << e.what() << std::endl;
    }
    ASSERT_TRUE(testing::internal::GetCapturedStdout() == "");
}

TEST(firstTest, myTest) {
    testing::internal::CaptureStdout();
    std::vector<Person> testUsers = {{"SamST", "Samuel",     "Stone",    32},
                                     {"Globe", "Globus",     "Kvadratov", 1},
                                     {"VIP",   "Vyacheslav", "Alehin",    7},
                                     {"KAR",   "Roman",      "Kazakov",  65},
                                     {"PePe",  "Petr",       "Petrov",   23},
                                     {"Navi",  "Ivan",       "Ivanov",   15}};
    for (int i = 0; i < testUsers.size(); i++) 
        Add(testUsers[i]);
    sleep(10); 
    for (int i = 0; i < testUsers.size(); i++) 
        if (!Search(testUsers[i]))
            std::cout << "ERROR: can\'t find user " << i << std::endl;
    ASSERT_TRUE(testing::internal::GetCapturedStdout() == "");
}

int main(int argc, char *argv[]) {
    if (argc != 7)
        std::cout << "ERROR: not enough argc" << std::endl;
    std::map<std::string, std::string> argMap;
    for (int i = 1; i < argc; i++) {
        std::string arg(argv[i]);
        int j = std::find(arg.begin(), arg.end(), '=') - arg.begin();
        if (j == arg.size()) {
            std::cout << "ERROR in argc[" + std::to_string(i) + "]" << std::endl;
            return 0;
        }
        argMap[arg.substr(0, j)] = arg.substr(j + 1, arg.size() - j);
    }
    NConnect::ip     = GetSingInData(argMap, "ip");
    NConnect::log    = GetSingInData(argMap, "log");
    NConnect::pass   = GetSingInData(argMap, "pass");
    NConnect::db     = GetSingInData(argMap, "db");
    NConnect::dbPort = std::stoi(GetSingInData(argMap, "dbPort"));
    if (NConnect::ip == "-1" || NConnect::log == "-1" || NConnect::pass == "-1" || NConnect::db == "-1" || NConnect::dbPort == -1) {
        std::cout << "Bad input! Shutting down..." << std::endl;
        return 0;
    }
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
