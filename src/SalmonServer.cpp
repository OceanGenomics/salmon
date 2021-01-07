#include "SalmonServer.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <signal.h>
#include <sys/socket.h>
#include <sys/types.h> /* See NOTES */
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <map>

#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <utility>
namespace po = boost::program_options;

static void serveIndex(const std::string& socketPath, const std::string& indexPath,
                int& argc, argvtype& argv,
                std::unique_ptr<SalmonIndex>& salmonIndex);
static void serverMainLoop(int&argc, argvtype& argv, int unix_socket);
static void contactServer(const std::string& socketPath, int argc, argvtype argv);

void salmonServer(int& argc, argvtype& argv, std::unique_ptr<SalmonIndex>& salmonIndex) {
  using std::string;
  po::options_description servero("server options");
  servero.add_options()
    ("index,i", po::value<string>(), "salmon index")
    ("server", po::value<string>(), "server socket path");

  po::command_line_parser parser{argc, argv};
  parser.options(servero).allow_unregistered();
  auto parsed = parser.run();
  po::variables_map vm;
  store(parsed, vm);

  if(!vm.count("server")) // No --server, do nothing
    return;

  if(vm.count("index"))
    serveIndex(vm["server"].as<string>(), vm["index"].as<string>(), argc, argv, salmonIndex);
  else
    contactServer(vm["server"].as<string>(), argc, argv);
}

void cpperror(const char* msg) {
  throw std::runtime_error(std::string(msg) + ": " + strerror(errno));
}

struct deferClose {
  int fd_;
  deferClose(int fd) : fd_(fd) {}
  ~deferClose() { close(fd_); }
};
// Cleanup, but only if we are the parent process
struct deferUnlink {
  static bool parent;
  const char* path_;
  deferUnlink(const char* path) : path_(path) {}
  ~deferUnlink() { if(parent) unlink(path_); }
};
bool deferUnlink::parent = true;

void serveIndex(const std::string& socketPath,
                const std::string& indexPath,
                int& argc, argvtype& argv,
                std::unique_ptr<SalmonIndex>& salmonIndex) {

  // Create unix socket and bind it
  struct sockaddr_un addr;
  if(socketPath.size() >= sizeof(addr.sun_path) - 1)
    cpperror("Path for server socket is too long");
  int unix_socket = socket(AF_UNIX, SOCK_STREAM, 0);
  if(unix_socket == -1)
    cpperror("Failed to open the server socket");
  deferClose closeSocket(unix_socket);

  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, socketPath.c_str());
  if(bind(unix_socket, (struct sockaddr*)&addr, sizeof(addr)) == -1)
    cpperror("Failed to bind server socket");
  deferUnlink unlinkSocket(addr.sun_path);

  // Load index
  boost::filesystem::path indexDirectory(indexPath);
  auto consoleSink =
      std::make_shared<spdlog::sinks::ansicolor_stderr_sink_mt>();
  consoleSink->set_color(spdlog::level::warn, consoleSink->magenta);
  auto consoleLog = spdlog::create("stderrLog", {consoleSink});

  salmonIndex = checkLoadIndex(indexDirectory, consoleLog);

  // This only returns in a child process to continue execution of Salmon
  serverMainLoop(argc, argv, unix_socket);
}

static volatile bool done = false;
void term_handler(int) { done = true; }
void chld_handler(int) { /* Do nothing */ }
void setupSignals() {
  struct sigaction act;
  memset(&act, '\0', sizeof(act));
  act.sa_handler = term_handler;
  if(sigaction(SIGINT, &act, nullptr) == -1 || sigaction(SIGTERM, &act, nullptr) == -1)
    cpperror("Error redirecting termination signals");
  act.sa_handler = chld_handler;
  if(sigaction(SIGCHLD, &act, nullptr) == -1)
    cpperror("Error redirecting SIGCHLD");
}

// When a child is done (process waited for), send it it's status and close
// socket
void handleDoneChildren(std::map<pid_t,int>& childrenSocket) {
  while(true) {
    int status;
    pid_t pid = wait(&status);
    if(pid == -1) {
      if(errno == ECHILD)
        break; // No child waiting. Done
      if(errno == EINTR)
        continue;
      std::cerr << "Warning: error while waiting for a child: " << strerror(errno) << std::endl;
      break;
    }
    auto it = childrenSocket.find(pid);
    if(it == childrenSocket.end()) {
      std::cerr << "Warning: caught unknown child process " << pid << std::endl;
      continue;
    }
    while(true) {
      auto sent = send(it->second, &status, sizeof(status), 0);
      if(sent == -1) {
        if(errno == EINTR)
          continue;
        std::cerr << "Warning: failed to send status to process " << pid << strerror(errno) << std::endl;
      }
    }
    close(it->second);
    childrenSocket.erase(it);
  }
}

void handleChild(int fd, int& argc, argvtype& argv);
void serverMainLoop(int& argc, argvtype& argv, int unix_socket) {
  setupSignals();

  if(listen(unix_socket, 5) == -1)
    cpperror("Error listening on unix socket");

  std::map<pid_t,int> childrenSocket;

  while(!done) {
    handleDoneChildren(childrenSocket);

    struct sockaddr_un addr;
    socklen_t          addrlen;
    int                fd = accept(unix_socket, (struct sockaddr*)&addr, &addrlen);
    if(fd == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Error accepting on unix socket");
    }

    pid_t pid = fork();
    switch(pid) {
    case -1:
      std::cerr << "Warning: failed to create child process: " << strerror(errno) << std::endl;
      close(fd); // Summary termination error sent to client
      break;

    case 0:
      deferUnlink::parent = false;
      handleChild(fd, argc, argv);
      return;

    default:
      childrenSocket[pid] = fd;
      break;
    }
  }

  std::cerr << "Wait for remaining children" << std::endl;
  handleDoneChildren(childrenSocket);
  exit(EXIT_SUCCESS);
}

void resetSignal() {
  struct sigaction act;
  memset(&act, '\0', sizeof(act));
  act.sa_handler = SIG_DFL;
  sigaction(SIGINT, &act, nullptr);
  sigaction(SIGTERM, &act, nullptr);
}

// In child, redirect outputs, update argc and argv, then continue processing.
// In case of error, exit(1). Parent will send error to client.
static constexpr size_t size = 1024 * 1024 - 2; // Maximum argv size
static std::vector<char> rawArgv;
static std::vector<const char*> childArgv;
void handleChild(int fd, int& argc, argvtype& argv) {
  resetSignal();
  // XXX TBD receive and redirect stdout / stderr
  rawArgv.resize(size+2, '\0'); // +1 to make sure that there is 2 '\0' at the end
  size_t offset = 0;

  while(true) {
    ssize_t received = recv(fd, rawArgv.data() + offset, size - offset, 0);
    if(received == -1) {
      if(errno == EINTR)
        continue;
      std::cerr << "Warning: failed to received argument from client: " << strerror(errno);
      exit(1);
    }
    if(received == 0)
      break;
    offset += received;
    if(offset == size) {
      std::cerr << "Warning: command line max length exceeded";
      exit(1);
    }
  }

  const char* cur = rawArgv.data();
  while(cur < rawArgv.data() + offset) {
    childArgv.push_back(cur);
    cur += strlen(cur);
  }
  argc = childArgv.size();
  argv = childArgv.data();
}

void contactServer(const std::string& socketPath, int argc, argvtype argv) {
  // Create unix socket and connect it
  struct sockaddr_un addr;
  if (socketPath.size() >= sizeof(addr.sun_path) - 1)
    cpperror("Path for server socket is too long");
  int unixSocket = socket(AF_UNIX, SOCK_STREAM, 0);
  if (unixSocket == -1)
    cpperror("Failed to open the server socket");
  deferClose closeSocket(unixSocket);
  if(connect(unixSocket, (struct sockaddr*)&addr, sizeof(addr)) == -1)
    cpperror("Failed to connect to server");

  // Send my arguments
  size_t argv_len = 0;
  for(int i = 0; i < argc; ++i)
    argv_len += strlen(argv[i]);
  size_t offset = 0;
  while(offset < argv_len) {
    ssize_t sent = send(unixSocket, argv[0] + offset, argv_len - offset, 0);
    if(sent == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Failed to send arguments to server");
    }
    offset += argv_len;
  }

  // Wait for return status
  int status;
  while(true) {
    ssize_t received = recv(unixSocket, &status, sizeof(status), 0);
    if(received == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Failed to get return status from server");
    }
    break;
  }
  exit(status);
}
