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

std::string GetSingInData(std::map<std::string, std::string> argMap, std::string data) {
    if (argMap.find("--" + data) == argMap.end()) {
        std::cout << "ERROR: no " << data << " in arg" << std::endl;
        return "-1";
    }
    return argMap["--" + data];
}

int main(int argc, char* argv[]) {
    if (argc != 8)
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
    NConnect::log     = GetSingInData(argMap, "log");
    NConnect::pass    = GetSingInData(argMap, "pass");
    NConnect::db      = GetSingInData(argMap, "db");
    NConnect::queue   = GetSingInData(argMap, "queue");
    NConnect::topic   = GetSingInData(argMap, "topic");
    NConnect::dbPort  = std::stoi(GetSingInData(argMap, "dbPort"));
    NConnect::idGroup = std::stoi(GetSingInData(argMap, "idGroup"));
    if (NConnect::log == "-1" || NConnect::pass == "-1" || NConnect::db == "-1" || NConnect::queue == "-1"
        || NConnect::topic == "-1" || NConnect::dbPort == -1 || NConnect::idGroup == -1) {
        std::cout << "Bad input! Shutting down..." << std::endl;
        return 0;
    }
    try {
        signal(SIGINT, [](int){NKafka::exe = false;});
        cppkafka::Configuration kafkaConfig = {{"metadata.broker.list", NConnect::queue}, 
                                               {"group.id", std::to_string(NConnect::idGroup)}, {"enable.auto.commit", false}};
        cppkafka::Consumer kafkaConsumer(kafkaConfig);
        kafkaConsumer.set_assignment_callback([](const cppkafka::TopicPartitionList& kafkaPart) {std::cout << "Got assigned: " 
                                                                                                           << kafkaPart << std::endl;});
        kafkaConsumer.set_revocation_callback([](const cppkafka::TopicPartitionList& kafkaPart) {std::cout << "Got revoked: " 
                                                                                                           << kafkaPart << std::endl;});
        kafkaConsumer.subscribe({NConnect::topic});
        std::cout << "Getting POST requests from topic " << NConnect::topic << std::endl;
        while (NKafka::exe) {
            cppkafka::Message req = kafkaConsumer.poll();
            if (req) {
                if (req.get_error()) {
                    if (!req.is_eof())
                        std::cout << "Error: bad request: " << req.get_error() << std::endl;
                }
                else {
                    if (req.get_key())
                        std::cout << req.get_key() << " -> ";
                    std::string payload = req.get_payload();
                    std::cout << req.get_payload() << std::endl;
                    Poco::JSON::Parser jsonParser;
                    bool fail = false;
                    Person user;
                    try {
                        Poco::Dynamic::Var parsed = jsonParser.parse(payload);
                        Poco::JSON::Object::Ptr jsonObj = parsed.extract<Poco::JSON::Object::Ptr>();
                        user.login = jsonObj->get("login").toString();
                        user.first_name = jsonObj->get("first_name").toString();
                        user.last_name = jsonObj->get("last_name").toString();
                        jsonObj->get("age").convert(user.age);
                    }
                    catch (...) {
                        fail = true;
                        std::cout << "Error: bad parse: " << payload << std::endl;
                        continue;
                    }
                    auto sqlSession = std::unique_ptr<Poco::Data::Session>(ConnectorSQL());
                    auto& session = *sqlSession;
                    try {
                        Poco::Data::Statement requestSQL(session);
                        requestSQL << "SELECT login FROM Person WHERE login=?", Poco::Data::Keywords::use(user.login),
                                       Poco::Data::Keywords::range(0, 1);
                        requestSQL.execute();
                        Poco::Data::RecordSet recordSet(requestSQL);
                        if (recordSet.moveFirst()) 
                            fail = true;
                    }
                    catch (Poco::Data::MySQL::ConnectionException& e) {
                        std::cout << "connection ERROR:" << e.what() << std::endl;
                    }
                    catch (Poco::Data::MySQL::StatementException& e) {
                        std::cout << "statement ERROR:" << e.what() << std::endl;
                    }
                    if (!fail) {
                        try {
                            Poco::Data::Statement requestSQL(session);
                            requestSQL << "INSERT INTO Person (login, first_name, last_name, age)VALUES (?, ?, ?, ?)",
                                           Poco::Data::Keywords::use(user.login), Poco::Data::Keywords::use(user.first_name), 
                                           Poco::Data::Keywords::use(user.last_name), Poco::Data::Keywords::use(user.age),
                                           Poco::Data::Keywords::range(0, 1);
                            requestSQL.execute();
                        }
                        catch (Poco::Data::MySQL::ConnectionException& e) {
                            std::cout << "connection ERROR:" << e.what() << std::endl;
                        }
                        catch (Poco::Data::MySQL::StatementException& e) {
                            std::cout << "statement ERROR:" << e.what() << std::endl;
                        }
                    }
                    kafkaConsumer.commit(req);
                }
            }
        }
    }
    catch (const std::exception& e) {
        std::cerr << e.what() << std::endl;
    }
    return 1;
}