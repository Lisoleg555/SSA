#include <iostream>
#include <vector>
#include <string>
#include <stdio.h>
#include <fstream>
#include <memory>
#include <exception>
#include <thread>
#include <sstream>
#include <map> 
#include <functional>
#include <algorithm>

#include "Poco/Exception.h"
#include "Poco/StreamCopier.h"
#include "Poco/JSON/Object.h"
#include "Poco/JSON/Parser.h"
#include "Poco/Thread.h"
#include "Poco/ThreadPool.h"
#include "Poco/Runnable.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/Timestamp.h"
#include "Poco/Dynamic/Var.h"

#include <Poco/Data/RecordSet.h>
#include <Poco/Data/Session.h>
#include <Poco/Data/SessionFactory.h>
#include <Poco/Data/MySQL/Connector.h>
#include <Poco/Data/MySQL/MySQLException.h>
#include <Poco/Data/Statement.h>

#include "Poco/Util/ServerApplication.h"
#include "Poco/Util/Option.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/HelpFormatter.h"

#include "Poco/Net/HTTPServerParams.h"
#include "Poco/Net/HTTPServerRequest.h"
#include "Poco/Net/HTTPServerResponse.h"
#include "Poco/Net/HTTPServer.h"
#include "Poco/Net/HTTPRequestHandler.h"
#include "Poco/Net/HTTPRequestHandlerFactory.h"

#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/SocketStream.h"
#include "Poco/Net/HTMLForm.h"

#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/cache/cache_peek_mode.h>

typedef struct TPerson {   
    std::string login;
    std::string first_name;
    std::string last_name;
    int age;
} Person;

namespace NConnect {
    const std::string host = "127.0.0.1";
    std::string ip    = "";
    std::string log   = "";
    std::string db    = "";
    std::string pass  = "";
    std::string cache = "";
    const int port    = 8080;
    int dbPort  = -1;
}

namespace NHtml {
    const std::string start = "<html lang=\"ru\"><head><title>Server</title></head><body><h1>";
    const std::string end = "</h1></body></html>";
}

namespace NIgniteCache {
    ignite::thin::IgniteClient client;
    ignite::thin::cache::CacheClient<std::string, std::string> cache;
}
