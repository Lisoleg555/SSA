#include <unistd.h>
#include <gtest/gtest.h>
#include "lib.h"

Poco::Data::Session* ConnectorSQL() {
    std::string keyConnect = "host=" + NConnect::host + ";user=" + NConnect::log + ";db=" + NConnect::db + ";password=" 
                           + NConnect::pass;
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

TEST(clearDBTest,  myTest) {
    testing::internal::CaptureStdout();
    auto sqlSession = ConnectorSQL();
    Poco::Data::Session& session = *sqlSession;
    try {                                                           
        Poco::Data::Statement checkTable(session);
        Poco::Data::Statement requestSQL(session);
        checkTable << "DROP TABLE IF EXISTS Person;"; 
        requestSQL << "CREATE TABLE IF NOT EXISTS Person ("
                   << "    login VARCHAR(20) CHARACTER SET utf8 COLLATE utf8_unicode_ci NOT NULL PRIMARY KEY,"
                   << "    first_name VARCHAR(15) CHARACTER SET utf8 COLLATE utf8_unicode_ci  NULL,"
                   << "    last_name VARCHAR(30) CHARACTER SET utf8 COLLATE utf8_unicode_ci  NOT NULL,"
                   << "    age INT NULL CHECK(age >= 0)"
                   << ");";
        checkTable.execute();
        requestSQL.execute();
    }                                                               
    catch (Poco::Data::MySQL::ConnectionException& e) {             
        std::cout << "connection ERROR:" << e.what() << std::endl;  
    }                                                               
    catch (Poco::Data::MySQL::StatementException& e) {              
        std::cout << "statement ERROR:" << e.what() << std::endl;   
    }
    delete sqlSession;
    ASSERT_TRUE(testing::internal::GetCapturedStdout() == "");
}

TEST(firstTest, myTest) {
    std::vector<Person> testUsers = {{"SamST", "Samuel",     "Stone",    32},
                                     {"Globe", "Globus",     "Kvadratov", 1},
                                     {"VIP",   "Vyacheslav", "Alehin",    7},
                                     {"KAR",   "Roman",      "Kazakov",  65},
                                     {"PePe",  "Petr",       "Petrov",   23},
                                     {"Navi",  "Ivan",       "Ivanov",   15},};
    for (int i = 0; i < testUsers.size(); i++) {
        Poco::Net::SocketAddress addr(NConnect::ip, NConnect::port);
        Poco::Net::StreamSocket socket(addr);
        Poco::Net::SocketStream requestPost(socket);
        requestPost << "POST /person HTTP/1.1\ncontent-type: application/url-encoded\n\nadd=True&login=" << testUsers[i].login 
                    << "&first_name=" << testUsers[i].first_name << "&last_name=" << testUsers[i].last_name << "&age=" 
                    << testUsers[i].age << std::endl;
        requestPost.flush();
    }
    sleep(5);
    testing::internal::CaptureStdout(); 
    auto sqlSession = ConnectorSQL();
    Poco::Data::Session& session = *sqlSession;
    for (int i = 0; i < testUsers.size(); i++) {
        Person testUser;
        try {                                                           
            Poco::Data::Statement requestSQL(session);
            requestSQL << "SELECT login, first_name, last_name, age FROM Person WHERE login=? AND first_name=? AND last_name=? "
                       << "AND age=?;", Poco::Data::Keywords::into(testUser.login), Poco::Data::Keywords::into(testUser.first_name),
                           Poco::Data::Keywords::into(testUser.last_name), Poco::Data::Keywords::into(testUser.age), 
                           Poco::Data::Keywords::use(testUsers[i].login), Poco::Data::Keywords::use(testUsers[i].first_name),
                           Poco::Data::Keywords::use(testUsers[i].last_name), Poco::Data::Keywords::use(testUsers[i].age),
                           Poco::Data::Keywords::range(0, 1);
            requestSQL.execute();
            Poco::Data::RecordSet recordSet(requestSQL);
            ASSERT_TRUE(recordSet.moveFirst());
        }                                                               
        catch (Poco::Data::MySQL::ConnectionException& e) {             
            std::cout << "connection ERROR:" << e.what() << std::endl;  
        }                                                               
        catch (Poco::Data::MySQL::StatementException& e) {              
            std::cout << "statement ERROR:" << e.what() << std::endl;   
        }
    }
    delete sqlSession;
    ASSERT_TRUE(testing::internal::GetCapturedStdout() == "");
}

int main(int argc, char *argv[]) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
