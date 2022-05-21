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

bool PrefixCompare(const std::string& URI, const std::string& per) {
    if (per.size() > URI.size())
        return false;
    for (int i = 0; i < per.size(); i++)
        if (URI[i] != per[i])
            return false;
    return true;
}

std::string GetSingInData(std::map<std::string, std::string> argMap, std::string data) {
    if (argMap.find("--" + data) == argMap.end()) {
        std::cout << "ERROR: no " << data << " in arg" << std::endl;
        return "-1";
    }
    return argMap["--" + data];
}

class TSQLResponse : public Poco::Net::HTTPRequestHandler {
public:
    TSQLResponse(const std::string& form) : formPri(form) {}

    void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        bool fail = false;
        Person user;
        Poco::Net::HTMLForm form(req, req.stream());
        res.setChunkedTransferEncoding(true);
        res.setContentType("application/json");
        std::ostream& myOstream = res.send();
        std::string getPost = req.getMethod();
        if (getPost == "GET" && form.has("login")) {
            std::string login = form.get("login");
            auto sqlSession = std::unique_ptr<Poco::Data::Session>(ConnectorSQL());
            auto& session = *sqlSession;
            try {
                Poco::Data::Statement requestSQL(session);
                requestSQL << "SELECT login, first_name, last_name, age FROM Person WHERE login=?",
                               Poco::Data::Keywords::into(user.login), Poco::Data::Keywords::into(user.first_name), 
                               Poco::Data::Keywords::into(user.last_name), Poco::Data::Keywords::into(user.age), 
                               Poco::Data::Keywords::use(login), Poco::Data::Keywords::range(0, 1);
                requestSQL.execute();
                Poco::Data::RecordSet recordSet(requestSQL);
                if (!recordSet.moveFirst()) 
                    throw std::logic_error("not found");
            }
            catch (Poco::Data::MySQL::ConnectionException& e) {
                std::cout << "connection ERROR:" << e.what() << std::endl;
            }
            catch (Poco::Data::MySQL::StatementException& e) {
                std::cout << "statement ERROR:" << e.what() << std::endl;
            }
            catch(std::logic_error& e) {
                std::cout << login << " not found" << std::endl;
                fail = true;
            }
            try {
                Poco::JSON::Object::Ptr jsonObj = new Poco::JSON::Object();
                if (!fail) {
                    jsonObj->set("login", user.login);
                    jsonObj->set("first_name", user.first_name);
                    jsonObj->set("last_name", user.last_name);
                    jsonObj->set("age", user.age);
                }
                std::stringstream tmpStr;
                Poco::JSON::Stringifier::stringify(jsonObj, tmpStr);
                std::string jsonRes = tmpStr.str();
                myOstream << jsonRes;
            }
            catch (...) {
                std::cout << "exception" << std::endl;
                res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_INTERNAL_SERVER_ERROR);
                return;
            }
            res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK);
        }
        else if (getPost == "GET" && form.has("first_name") && form.has("last_name")) {
            std::string firstName = form.get("first_name");
            std::string lastName  = form.get("last_name");
            auto sqlSession = std::unique_ptr<Poco::Data::Session>(ConnectorSQL());
            Poco::Data::Session& session = *sqlSession;
            std::vector<Person> result;
            try {
                Poco::Data::Statement requestSQL(session);
                requestSQL << "SELECT login, first_name, last_name, age FROM Person WHERE first_name LIKE ? AND last_name LIKE ?", 
                               Poco::Data::Keywords::into(user.login), Poco::Data::Keywords::into(user.first_name), 
                               Poco::Data::Keywords::into(user.last_name), Poco::Data::Keywords::into(user.age), 
                               Poco::Data::Keywords::use(firstName), Poco::Data::Keywords::use(lastName), 
                               Poco::Data::Keywords::range(0, 1);
                while (!requestSQL.done())
                    if (requestSQL.execute())
                        result.push_back(user);
            }
            catch (Poco::Data::MySQL::ConnectionException& e) {
                std::cout << "connection ERROR:" << e.what() << std::endl;
            }
            catch (Poco::Data::MySQL::StatementException& e) {
                std::cout << "statement ERROR:" << e.what() << std::endl;
            }
            try {
                Poco::JSON::Array jsonReq;
                for (int i = 0; i < result.size(); i++) {
                    Poco::JSON::Object::Ptr jsonObj = new Poco::JSON::Object();
                    jsonObj->set("login", result[i].login);
                    jsonObj->set("first_name", result[i].first_name);
                    jsonObj->set("last_name", result[i].last_name);
                    jsonObj->set("age", result[i].age);
                    jsonReq.add(jsonObj);
                }
                Poco::JSON::Stringifier::stringify(jsonReq, myOstream);
            }
            catch (...) {
                std::cout << "exception" << std::endl;
                res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_INTERNAL_SERVER_ERROR);
                return;
            }
            res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK);
        }
        else if (getPost == "POST" && form.has("login") && form.has("last_name") && form.has("first_name") && form.has("age")) {
            std::string log       = form.get("login");
            std::string lastName  = form.get("last_name");
            std::string firstName = form.get("first_name");
            int age = std::stoi(form.get("age").c_str());
            static cppkafka::Configuration kafkaConfig = {{"metadata.broker.list", NConnect::queue}};
            static cppkafka::Producer kafkaProducer(kafkaConfig);
            Poco::JSON::Object::Ptr jsonObj = new Poco::JSON::Object();
            jsonObj->set("login", log);
            jsonObj->set("first_name", firstName);
            jsonObj->set("last_name", lastName);
            jsonObj->set("age", age);
            std::stringstream tmpStr;
            Poco::JSON::Stringifier::stringify(jsonObj, tmpStr);
            std::string message = tmpStr.str();
            while (!fail) {
                try {
                    kafkaProducer.produce(cppkafka::MessageBuilder(NConnect::topic).partition(0).payload(message));
                    fail = true;
                }
                catch (...) {}
            }
            jsonObj = new Poco::JSON::Object();
            jsonObj->set("status", "OK");
            Poco::JSON::Stringifier::stringify(jsonObj, myOstream);
            res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK);
        }
    }

private:
    std::string formPri;
};

class TWebResponse : public Poco::Net::HTTPRequestHandler {
public:
    TWebResponse(const std::string& form) : formPri(form) {}

    void handleRequest(Poco::Net::HTTPServerRequest& req, Poco::Net::HTTPServerResponse& res) {
        res.setChunkedTransferEncoding(true);
        res.setContentType("text/html");
        std::ostream& myOstream = res.send();
        std::string uri = req.getURI();
        std::string fileDir = ".." + uri;
        FILE* file = fopen(fileDir.c_str(), "rb");
        if (!file) {
            std::cout << "path " << fileDir << " not found" << std::endl;
            myOstream << NHtml::start << "Error 404" << NHtml::end;
            res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_NOT_FOUND);
        }
        else {
            std::vector<char> lines(400);
            int linesSize = lines.size() - 1;
            int size = 0;
            while (size = fread(lines.data(), sizeof(char), linesSize, file)) {
                lines[size] = '\0';
                myOstream << lines.data();
            }
            fclose(file);
            res.setStatus(Poco::Net::HTTPResponse::HTTPStatus::HTTP_OK);
        }
    }

private:
    std::string formPri;
};

class THandler : public Poco::Net::HTTPRequestHandlerFactory {
public:
    THandler(const std::string& form) : formPri(form) {}

    Poco::Net::HTTPRequestHandler* createRequestHandler(const Poco::Net::HTTPServerRequest& req) {
        std::cout << "Request " + req.getMethod() + " " +  req.getURI() + " from " + req.clientAddress().toString() << std::endl;
        if (PrefixCompare(req.getURI(), "/person"))
            return new TSQLResponse(formPri);
        return new TWebResponse(formPri);
    }

private:
    std::string formPri;
};

class TServer : public Poco::Util::ServerApplication {
public:
    TServer() : badStart(false) {}

    ~TServer() {}

protected:
    void Init(Poco::Util::Application& self) {
        loadConfiguration();
        Poco::Util::ServerApplication::initialize(self);
    }

    void Ops(Poco::Util::OptionSet& options) {
        Poco::Util::ServerApplication::defineOptions(options);
    }

    int main(const std::vector<std::string>& args) {
        if (!badStart) {
            unsigned short port = (unsigned short) config().getInt("HTTPWebServer.port", NConnect::port);
            std::string form(config().getString("HTTPWebServer.format", Poco::DateTimeFormat::SORTABLE_FORMAT));
            Poco::Net::ServerSocket serverSocket(Poco::Net::SocketAddress("0.0.0.0", port));
            Poco::Net::HTTPServer httpServer(new THandler(form), serverSocket, new Poco::Net::HTTPServerParams);
            std::cout << "Started server on " << NConnect::ip << ":" << std::to_string(port) << std::endl;
            httpServer.start();
            waitForTerminationRequest();
            httpServer.stop();
        }
        return Poco::Util::Application::EXIT_OK;
    }

    void Uninit() {
        Poco::Util::ServerApplication::uninitialize();
    }

private:
    bool badStart;
};

int main(int argc, char* argv[]) {
    TServer app;
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
    NConnect::ip     = GetSingInData(argMap, "ip");
    NConnect::log    = GetSingInData(argMap, "log");
    NConnect::pass   = GetSingInData(argMap, "pass");
    NConnect::db     = GetSingInData(argMap, "db");
    NConnect::queue  = GetSingInData(argMap, "queue");
    NConnect::topic  = GetSingInData(argMap, "topic");
    NConnect::dbPort = std::stoi(GetSingInData(argMap, "dbPort"));
    if (NConnect::ip == "-1" || NConnect::log == "-1" || NConnect::pass == "-1" || NConnect::db == "-1" 
        || NConnect::queue == "-1" || NConnect::topic == "-1" || NConnect::dbPort == -1) {
        std::cout << "Bad input! Shutting down..." << std::endl;
        return 0;
    }
    return app.run(1, argv);
}