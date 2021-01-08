#include "SalmonServer.hpp"
#include <asm-generic/socket.h>
#include <bits/types/struct_iovec.h>
#include <boost/program_options/parsers.hpp>
#include <cerrno>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <signal.h>
#include <stdexcept>
#include <sys/socket.h>
#include <sys/types.h> /* See NOTES */
#include <sys/un.h>
#include <sys/wait.h>
#include <unistd.h>
#include <map>
#include <poll.h>

#include <boost/program_options.hpp>
#include <boost/program_options/options_description.hpp>
#include <utility>
namespace po = boost::program_options;

// Same error status as salmoneServer
static int serveIndex(const std::string& socketPath, const std::string& indexPath,
                int& argc, argvtype& argv,
                std::unique_ptr<SalmonIndex>& salmonIndex);
static int serverMainLoop(int&argc, argvtype& argv, int unix_socket);
static int contactServer(const std::string& socketPath, const std::vector<std::string>& opts);

int salmonServer(int& argc, argvtype& argv, std::unique_ptr<SalmonIndex>& salmonIndex) {
  using std::string;
  po::options_description servero("server options");
  servero.add_options()
    ("index,i", po::value<string>(), "salmon index")
    ("server", po::value<string>(), "server socket path")
    ("args", po::value<std::vector<std::string>>(), "Args");
  po::positional_options_description pd;
  pd.add("args", -1);
  po::command_line_parser parser{argc, argv};
  parser.options(servero).positional(pd).allow_unregistered();
  auto parsed = parser.run();
  po::variables_map vm;
  store(parsed, vm);

  if(!vm.count("server")) // No --server, do nothing
    return -1;

  try {
    if(vm.count("index"))
      return serveIndex(vm["server"].as<string>(), vm["index"].as<string>(), argc, argv, salmonIndex);
    else {
      auto opts = po::collect_unrecognized(parsed.options, po::include_positional);
      return contactServer(vm["server"].as<string>(), opts);
    }
  }catch(std::exception& e) {
    std::cerr << "Server error: " << e.what() << std::endl;
    return EXIT_FAILURE;
  }
  return EXIT_FAILURE; // Should never be reached
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
  deferUnlink(const char* path) : path_(path) { }
  ~deferUnlink() { if(parent) unlink(path_); }
};
bool deferUnlink::parent = true;

int serveIndex(const std::string& socketPath,
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
      std::make_shared<spdlog::sinks::stderr_sink_mt>();
  //  consoleSink->set_color(spdlog::level::warn, consoleSink->magenta);
  auto consoleLog = spdlog::create("servertderrLog", {consoleSink});
  salmonIndex = checkLoadIndex(indexDirectory, consoleLog);

  // This only returns in a child process to continue execution of Salmon
  return serverMainLoop(argc, argv, unix_socket);
}

static volatile bool done = false;
void term_handler(int) { done = true; }
void chld_handler(int) { /* Do nothing */ }

struct DefineSignals {
  struct sigaction actionInt, actionTerm, actionChld, actionPipe;

  void setup() {
    struct sigaction act;
    memset(&act, '\0', sizeof(act));
    act.sa_handler = term_handler;
    if (sigaction(SIGINT, &act, &actionInt) == -1 ||
        sigaction(SIGTERM, &act, &actionTerm) == -1)
      cpperror("Error redirecting termination signals");
    act.sa_handler = chld_handler;
    if (sigaction(SIGCHLD, &act, &actionChld) == -1)
      cpperror("Error ignoring SIGCHLD");
    if(sigaction(SIGPIPE, &act, &actionPipe) == -1)
      cpperror("Error ignore SIGPIPE");
  }

  void reset() {
    sigaction(SIGINT, &actionInt, nullptr);
    sigaction(SIGTERM, &actionTerm, nullptr);
    sigaction(SIGCHLD, &actionChld, nullptr);
    sigaction(SIGPIPE, &actionPipe, nullptr);
  }
};

//
// Server side, parent process
//
// The server accepts connections on the UNIX socket. For each new connection,
// it forks a new child process that will do the actual work. Keep track of the connection
// "child pid" -> "socket fd". When a child dies, write the exit status on the socket fd
// (for the client to report), then close the socket fd.
//
// If a socket fd hangs up (poll returns HUP), which means the client closed its side (e.g.,
// the client got Ctrl-C and died), then we send SIGTERM to the corresponding child process
// that is doing the work on behalf of this client.
//

// When a child is done (process waited for), send it it's status and close
// socket
void handleDoneChildren(std::map<pid_t,int>& childrenSocket) {
  while(true) {
    int status;
    pid_t pid = waitpid(-1, &status, WNOHANG);
    if(pid == 0)
      break; // No child has stopped
    if(pid == -1) {
      if(errno == EINTR)
        continue;
      if(errno == ECHILD)
        break;
      std::cerr << "Warning: error while waiting for a child: " << strerror(errno) << std::endl;
      break;
    }
    auto it = childrenSocket.find(pid);
    if(it == childrenSocket.end()) {
      std::cerr << "Warning: caught unknown child process " << pid << std::endl;
      continue;
    }

    if(it->second != -1) { // If -1, already closed by client, so can't send status
      while(true) {
        auto sent = send(it->second, &status, sizeof(status), 0);
        if(sent == -1) {
          if(errno == EINTR)
            continue;
          std::cerr << "Warning: failed to send status to process " << pid << ' ' << strerror(errno) << std::endl;
        }
        break;
      }
      close(it->second);
    }
    childrenSocket.erase(it);
  }
}

int handleChild(int fd, int& argc, argvtype& argv);
int serverMainLoop(int& argc, argvtype& argv, int unix_socket) {
  DefineSignals signals;
  signals.setup();

  std::cerr << "Server waiting for requests. Ctrl-C to stop." << std::endl;
  if(listen(unix_socket, 5) == -1)
    cpperror("Error listening on unix socket");

  std::map<pid_t,int> childrenSocket;
  std::vector<struct pollfd> pollfds;
  std::vector<pid_t> pollpids;

  while(!done) {
    handleDoneChildren(childrenSocket);

    pollfds.resize(childrenSocket.size() + 1);
    pollpids.resize(childrenSocket.size() + 1);
    pollfds[0].fd = unix_socket;
    pollfds[0].events = POLLIN;
    pollfds[0].revents = 0;
    int i = 1;
    for(const auto& child : childrenSocket) {
      pollfds[i].fd = child.second;
      pollfds[i].events = 0;
      pollfds[i].revents = 0;
      pollpids[i] = child.first;
      ++i;
    }

    // Can't use ppoll (not POSIX, not on mac) to avoid race conditions, use a timeout
    int res = poll(pollfds.data(), pollfds.size(), 10000);
    if(res == 0)
      continue;
    if(res == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Error polling file descriptors");
    }

    if(pollfds[0].revents & POLLIN) {
      // struct sockaddr_un addr;
      // socklen_t          addrlen;
      // std::cerr << "Unix socket pollin " << unix_socket << ' ' << pollfds[0].fd << std::endl;
      // system("ls -l /proc/self/fd");
      int fd = accept(pollfds[0].fd, nullptr, nullptr);
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
        signals.reset();
        for(const auto poll : pollfds)
          close(poll.fd);
        return handleChild(fd, argc, argv);
        break;

      default:
        childrenSocket[pid] = fd;
        break;
      }
    }

    for(size_t i = 1; i < pollfds.size(); ++i) {
      if(pollfds[i].revents & POLLHUP) { // Client closed it's socket. Forget about it
        close(pollfds[i].fd);
        kill(pollpids[i], SIGTERM);
        childrenSocket[pollpids[i]] = -1;
      }
    }
  }

  close(unix_socket);
  std::cerr << "Waiting for remaining children" << std::endl;
  handleDoneChildren(childrenSocket);
  return 0;
}

//
// Server side, Child process.
// 
// Receive arguments and file descriptor.
// After resetting the environment, return with -1 to keep on processing
// with the quantification
///

constexpr int numFds = 4;
static constexpr size_t size = 1024 * 1024; // Maximum argv size
static std::vector<char> rawArgv;
static std::vector<const char*> childArgv;
int handleChild(int fd, int& argc, argvtype& argv) {
  size_t offset = 0;

  int              fds[numFds];
  size_t           argvLen;
  struct iovec io           = {
    .iov_base               = &argvLen,
    .iov_len                = sizeof(argvLen)
  };
  union {
    char           buf[CMSG_SPACE(sizeof(fds))];
    struct cmsghdr align;
  } u;
  struct msghdr msg;
  memset(&msg, '\0', sizeof(msg));
  msg.msg_iov = &io;
  msg.msg_iovlen = 1;
  msg.msg_control = u.buf;
  msg.msg_controllen = sizeof(u.buf);
  while(true) {
    ssize_t        received = recvmsg(fd, &msg, 0);
    if(received == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Failed to receive client argument length");
    }
    if(received == 0)
      throw std::runtime_error("Premature closure of client socket");
    break;
  }
  if(argvLen >= size)
    throw std::runtime_error("Client argument length too long");
  struct cmsghdr* cmsg;
  for(cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
    if(cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SCM_RIGHTS) {
      memcpy(&fds, CMSG_DATA(cmsg), sizeof(fds));
      break;
    }
  }
  if(cmsg == nullptr)
    cpperror("Failed to receive client file descriptors");

  // Redirect the file descriptors
  if(fchdir(fds[3]))
    cpperror("Failed to change current working directories");
  close(fds[3]);
  for(int i = 0; i < 3; ++i) {
    if(dup2(fds[i], i) == -1)
      cpperror("Failed to redirect input/output");
    close(fds[i]);
  }

  // First copy the existing argv[0]. Then add a missing fake '-i /' (5 characters).
  // Then copy the arguments sent over by the client. Add 2 '\0' to make sure
  // there is an extra null character after the end of the last string (as there is
  // in the original argv).
  //
  // XXX: This type of arithmetic is error prone!
  size_t arg0Len = strlen(argv[0]);
  rawArgv.resize(argvLen + arg0Len + 5 + 2, '\0');
  char* cur = rawArgv.data();
  strcpy(cur, argv[0]);
  cur += arg0Len + 1;
  *cur++ = '-';
  *cur++ = 'i';
  ++cur;
  *cur++ = '/';
  ++cur;
  while(offset < argvLen) {
    ssize_t received = recv(fd, cur, argvLen - offset, 0);
    if(received == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Warning: failed to received argument from client");
    }
    if(received == 0)
      throw std::runtime_error("Premature closure of client socket");
    offset += received;
    cur += received;
  }

  cur = rawArgv.data();
  while(cur < rawArgv.data() + argvLen + arg0Len + 5) {
    childArgv.push_back(cur);
    cur += strlen(cur) + 1;
  }
  argc = childArgv.size();
  argv = childArgv.data();

  return -1;
}

//
// Client side
//
// Pack the command line argument and my file descriptor to send to the
// server (child process) via the UNIX socket. Then wait to receive status
// of quantification by reading the UNIX socket.
//

int sendBuffer(int socket, const char* buf, size_t size) {
  size_t offset = 0;
  while (offset < size) {
    ssize_t sent =
        send(socket, buf + offset, size - offset, 0);
    if (sent == -1) {
      if (errno == EINTR)
        continue;
      return -1;
    }
    offset += sent;
  }
  return size;
}

// Send command line arguments and stdint, stdout, stderr and current directory
void sendArguments(int unixSocket, const std::vector<std::string>& opts) {
  // Data for file descriptors
  int fdDir = open(".", O_RDONLY);
  if (fdDir == -1)
    cpperror("Failed top open current directory");
  int fds[numFds];
  fds[0] = STDIN_FILENO;
  fds[1] = STDOUT_FILENO;
  fds[2] = STDERR_FILENO;
  fds[3] = fdDir;

  // Linear copies of the arguments
  size_t argvLen = 0;
  for (const auto& opt : opts)
    argvLen += opt.size() + 1;

  std::vector<char> rawArgv(argvLen, '\0');
  char* cur = rawArgv.data();
  for (const auto& opt : opts) {
    strcpy(cur, opt.c_str());
    cur += opt.size() + 1;
  }

  // Collect argvLen and fds (as auxiliary) in a msg for sendmsg
  struct iovec io       = {
    .iov_base           = &argvLen,
    .iov_len            = sizeof(argvLen)
  };
  union {
    char           buf[CMSG_SPACE(sizeof(fds))];
    struct cmsghdr align;
  } u;
  struct msghdr    msg;
  memset(&msg, '\0', sizeof(msg));
  msg.msg_iov           = &io;
  msg.msg_iovlen        = 1;
  msg.msg_control       = u.buf;
  msg.msg_controllen    = sizeof(u.buf);
  struct cmsghdr*  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level      = SOL_SOCKET;
  cmsg->cmsg_type       = SCM_RIGHTS;
  cmsg->cmsg_len        = CMSG_LEN(sizeof(fds));
  memcpy(CMSG_DATA(cmsg), fds, sizeof(fds));

  while (true) {
    ssize_t sent = sendmsg(unixSocket, &msg, 0);
    if (sent == -1) {
      if (errno == EINTR)
        continue;
      cpperror("Failed to send client argument length");
    }
    break;
  }

  // Now send the content of rawArgv
  if(sendBuffer(unixSocket, rawArgv.data(), rawArgv.size()) == -1)
    cpperror("Failed to send arguments");
}

int contactServer(const std::string& socketPath, const std::vector<std::string>& opts) {
  // Create unix socket and connect it
  struct sockaddr_un addr;
  if (socketPath.size() >= sizeof(addr.sun_path) - 1)
    cpperror("Path for server socket is too long");
  int unixSocket = socket(AF_UNIX, SOCK_STREAM, 0);
  if (unixSocket == -1)
    cpperror("Failed to open the server socket");
  deferClose closeSocket(unixSocket);
  addr.sun_family = AF_UNIX;
  strcpy(addr.sun_path, socketPath.c_str());
  if(connect(unixSocket, (struct sockaddr*)&addr, sizeof(addr)) == -1)
    cpperror("Failed to connect to server");

  sendArguments(unixSocket, opts);

  // Wait for return status
  int status;
  while(true) {
    ssize_t received = recv(unixSocket, &status, sizeof(status), 0);
    if(received == -1) {
      if(errno == EINTR)
        continue;
      cpperror("Failed to get return status from server");
    }
    if(received == 0) {
      cpperror("Premature close from server");
    }
    break;
  }

  if(WIFEXITED(status))
    return WEXITSTATUS(status);
  if(WIFSIGNALED(status)) {
    struct sigaction act;
    memset(&act, '\0', sizeof(act));
    sigaction(WTERMSIG(status), &act, nullptr);
    kill(getpid(), WTERMSIG(status));
  }
  return EXIT_FAILURE;
}
