#include <iostream>
#include <vector>
#include <string>

#include "Poco/Exception.h"
#include "Poco/StreamCopier.h"
#include "Poco/JSON/Object.h"

#include <Poco/Data/RecordSet.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/OptionSet.h"

#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"

#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/Net/HTMLForm.h"

typedef struct TPerson {   
    std::string login;
    std::string first_name;
    std::string last_name;
    int age;
} Person;

namespace NConnect {
    const std::string host = "localhost";
    const std::string ip   = "127.0.0.1";
    const std::string log  = "stud";
    const std::string db   = "People";
    const std::string pass = "stud";
    const int port = 8080;
}

namespace NHtml {
    const std::string start = "<html lang=\"ru\"><head><title>Server</title></head><body><h1>";
    const std::string end = "</h1></body></html>";
}
