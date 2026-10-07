/*
 * server_6572.c
 * NetMessenger server - IE3010 Network Programming Assignment
 * Registration number: IT23816572
 * Port: 12572 (6000 + last 4 digits of registration number)
 * NID tag: 8165
 *
 * Concurrency model: single-process I/O multiplexing with select().
 * Chosen because the full set of connected clients, rooms and users
 * all live in one process's memory with no locking required, which
 * keeps the protocol's shared state (user list, room membership)
 * simple and race-free compared to a threaded or forked design.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <ctype.h>
#include <stdarg.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/stat.h>
#include <sys/types.h>

#define SERVER_PORT     12572
#define NID_TAG         "8165"
#define REGNO           "IT23816572"
#define LOG_FILE        "netmsg_IT23816572.log"
#define STORAGE_BASE    "./storage/IT23816572"

#define MAX_CLIENTS     64
#define MAX_ROOMS       64
#define MAX_USERNAME    64
#define MAX_ROOMNAME    64
#define MAX_FILENAME    256
#define MAX_LINE        1024
#define CBUF_SIZE       65536
#define MAX_FILE_BYTES  (50L * 1024 * 1024)   /* 50 MB cap */

typedef struct {
    int      fd;
    int      active;
    char     username[MAX_USERNAME];   /* empty until REGISTERed */

    char     buf[CBUF_SIZE];
    size_t   buf_len;

    int      in_file;                  /* 1 while receiving file bytes */
    int      file_discard;             /* 1 if file is being drained but rejected */
    long     file_size;
    long     file_received;
    char     file_name[MAX_FILENAME];
    char     file_sender[MAX_USERNAME];
    FILE    *file_fp;
    int      recipients[MAX_CLIENTS];
    int      recipient_count;

    int      quit_pending;
} Client;

typedef struct {
    char name[MAX_ROOMNAME];
    int  active;
    int  member_fd[MAX_CLIENTS];
    int  member_count;
} Room;

static Client clients[MAX_CLIENTS];
static Room   rooms[MAX_ROOMS];
static FILE  *logfp = NULL;

/* ---------- logging ---------- */

static void log_event(const char *fmt, ...) {
    if (!logfp) return;
    time_t t = time(NULL);
    struct tm tm_now;
    localtime_r(&t, &tm_now);
    char ts[32];
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tm_now);

    char msg[1024];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    fprintf(logfp, "[%s] %s\n", ts, msg);
    fflush(logfp);
}

/* ---------- small utils ---------- */

static ssize_t writen(int fd, const void *vptr, size_t n) {
    size_t nleft = n;
    const char *ptr = vptr;
    while (nleft > 0) {
        ssize_t nwritten = write(fd, ptr, nleft);
        if (nwritten <= 0) {
            if (nwritten < 0 && errno == EINTR) continue;
            return -1;
        }
        nleft -= nwritten;
        ptr += nwritten;
    }
    return n;
}

static void send_line(int fd, const char *text) {
    char out[MAX_LINE + 2];
    snprintf(out, sizeof(out), "%s\n", text);
    writen(fd, out, strlen(out));
}

static void send_ok(Client *c, const char *fmt, ...) {
    char body[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(body, sizeof(body), fmt, ap);
    va_end(ap);

    char out[768];
    snprintf(out, sizeof(out), "OK %s NID:%s", body, NID_TAG);
    send_line(c->fd, out);
}

static void send_err(Client *c, int code, const char *reason) {
    char out[256];
    snprintf(out, sizeof(out), "ERR %03d %s NID:%s", code, reason, NID_TAG);
    send_line(c->fd, out);
}

static void broadcast_msg(const char *line, int except_fd) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].username[0] != '\0' &&
            clients[i].fd != except_fd) {
            send_line(clients[i].fd, line);
        }
    }
}

static Client *find_client_by_username(const char *name) {
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && strcmp(clients[i].username, name) == 0)
            return &clients[i];
    }
    return NULL;
}

static Room *find_room(const char *name) {
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active && strcmp(rooms[i].name, name) == 0)
            return &rooms[i];
    }
    return NULL;
}

static Room *find_or_create_room(const char *name) {
    Room *r = find_room(name);
    if (r) return r;
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (!rooms[i].active) {
            rooms[i].active = 1;
            strncpy(rooms[i].name, name, MAX_ROOMNAME - 1);
            rooms[i].member_count = 0;
            return &rooms[i];
        }
    }
    return NULL; /* room table full */
}

static int room_has_member(Room *r, int fd) {
    for (int i = 0; i < r->member_count; i++)
        if (r->member_fd[i] == fd) return 1;
    return 0;
}

static void room_add_member(Room *r, int fd) {
    if (room_has_member(r, fd)) return;
    if (r->member_count < MAX_CLIENTS) r->member_fd[r->member_count++] = fd;
}

static void room_remove_member(Room *r, int fd) {
    for (int i = 0; i < r->member_count; i++) {
        if (r->member_fd[i] == fd) {
            r->member_fd[i] = r->member_fd[r->member_count - 1];
            r->member_count--;
            return;
        }
    }
}

static void remove_client_from_all_rooms(int fd) {
    for (int i = 0; i < MAX_ROOMS; i++)
        if (rooms[i].active) room_remove_member(&rooms[i], fd);
}

static int ensure_dir(const char *path) {
    char tmp[1024];
    snprintf(tmp, sizeof(tmp), "%s", path);
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755); /* may return -1/EEXIST, that's fine */
}

static void trim_trailing_cr(char *s) {
    size_t n = strlen(s);
    if (n > 0 && s[n - 1] == '\r') s[n - 1] = '\0';
}

/* ---------- command handling ---------- */

static void handle_register(Client *c, char *args) {
    char uname[MAX_USERNAME];
    if (sscanf(args, "%63s", uname) != 1 || uname[0] == '\0') {
        send_err(c, 400, "BAD_REGISTER");
        return;
    }
    if (find_client_by_username(uname)) {
        send_err(c, 1, "USERNAME_TAKEN");
        log_event("REGISTER FAILED fd=%d requested=%s reason=USERNAME_TAKEN", c->fd, uname);
        return;
    }
    strncpy(c->username, uname, MAX_USERNAME - 1);
    send_ok(c, "REGISTERED %s", c->username);
    log_event("REGISTER OK fd=%d username=%s", c->fd, c->username);

    char notice[160];
    snprintf(notice, sizeof(notice), "MSG PRESENCE JOIN %s", c->username);
    broadcast_msg(notice, c->fd);
}

static void handle_list(Client *c) {
    char list[2048] = "";
    int first = 1;
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].active && clients[i].username[0] != '\0') {
            if (!first) strncat(list, ",", sizeof(list) - strlen(list) - 1);
            strncat(list, clients[i].username, sizeof(list) - strlen(list) - 1);
            first = 0;
        }
    }
    send_ok(c, "USERS %s", list);
}

static void handle_bcast(Client *c, char *msg) {
    send_ok(c, "SENT");
    char out[MAX_LINE];
    snprintf(out, sizeof(out), "MSG BCAST %s %s", c->username, msg);
    broadcast_msg(out, c->fd);
    log_event("BCAST from=%s msg=\"%s\"", c->username, msg);
}

static void handle_pmsg(Client *c, char *args) {
    char target[MAX_USERNAME];
    int n;
    if (sscanf(args, "%63s%n", target, &n) != 1) {
        send_err(c, 400, "BAD_PMSG");
        return;
    }
    char *msg = args + n;
    while (*msg == ' ') msg++;

    Client *t = find_client_by_username(target);
    if (!t) {
        send_err(c, 2, "USER_NOT_FOUND");
        return;
    }
    send_ok(c, "SENT");
    char out[MAX_LINE];
    snprintf(out, sizeof(out), "MSG PRIV %s %s", c->username, msg);
    send_line(t->fd, out);
    log_event("PMSG from=%s to=%s msg=\"%s\"", c->username, target, msg);
}

static void handle_join(Client *c, char *args) {
    char room[MAX_ROOMNAME];
    if (sscanf(args, "%63s", room) != 1) {
        send_err(c, 400, "BAD_JOIN");
        return;
    }
    Room *r = find_or_create_room(room);
    if (!r) {
        send_err(c, 500, "ROOM_TABLE_FULL");
        return;
    }
    room_add_member(r, c->fd);
    send_ok(c, "JOINED %s", room);
    log_event("JOIN user=%s room=%s", c->username, room);
}

static void handle_leave(Client *c, char *args) {
    char room[MAX_ROOMNAME];
    if (sscanf(args, "%63s", room) != 1) {
        send_err(c, 400, "BAD_LEAVE");
        return;
    }
    Room *r = find_room(room);
    if (!r) {
        send_err(c, 3, "ROOM_NOT_FOUND");
        return;
    }
    room_remove_member(r, c->fd);
    send_ok(c, "LEFT %s", room);
    log_event("LEAVE user=%s room=%s", c->username, room);
}

static void handle_rooms(Client *c) {
    char list[2048] = "";
    int first = 1;
    for (int i = 0; i < MAX_ROOMS; i++) {
        if (rooms[i].active) {
            if (!first) strncat(list, ",", sizeof(list) - strlen(list) - 1);
            strncat(list, rooms[i].name, sizeof(list) - strlen(list) - 1);
            first = 0;
        }
    }
    send_ok(c, "ROOMS %s", list);
}

static void handle_rmsg(Client *c, char *args) {
    char room[MAX_ROOMNAME];
    int n;
    if (sscanf(args, "%63s%n", room, &n) != 1) {
        send_err(c, 400, "BAD_RMSG");
        return;
    }
    char *msg = args + n;
    while (*msg == ' ') msg++;

    Room *r = find_room(room);
    if (!r) {
        send_err(c, 3, "ROOM_NOT_FOUND");
        return;
    }
    send_ok(c, "SENT");
    char out[MAX_LINE];
    snprintf(out, sizeof(out), "MSG ROOM %s %s %s", room, c->username, msg);
    for (int i = 0; i < r->member_count; i++) {
        if (r->member_fd[i] != c->fd) send_line(r->member_fd[i], out);
    }
    log_event("RMSG from=%s room=%s msg=\"%s\"", c->username, room, msg);
}

/*
 * SENDFILE <target> <filename> <filesize>
 * Design decision (not fixed by the given protocol table): the recipient(s)
 * receive a header line "MSG FILE <sender> <filename> <filesize>" followed
 * immediately by exactly <filesize> raw bytes, mirroring the sender's own
 * SENDFILE framing. This is documented as an assumption in the report.
 */
static void handle_sendfile(Client *c, char *args) {
    char target[MAX_USERNAME > MAX_ROOMNAME ? MAX_USERNAME : MAX_ROOMNAME];
    char filename[MAX_FILENAME];
    long filesize;

    if (sscanf(args, "%63s %255s %ld", target, filename, &filesize) != 3) {
        send_err(c, 400, "BAD_SENDFILE");
        return;
    }

    /* Resolve target: try user first, then room */
    Client *tu = find_client_by_username(target);
    Room *tr = tu ? NULL : find_room(target);

    if (!tu && !tr) {
        send_err(c, 2, "USER_NOT_FOUND");
        return;
    }

    c->recipient_count = 0;
    if (tu) {
        c->recipients[c->recipient_count++] = tu->fd;
    } else {
        for (int i = 0; i < tr->member_count; i++) {
            if (tr->member_fd[i] != c->fd)
                c->recipients[c->recipient_count++] = tr->member_fd[i];
        }
    }

    c->in_file = 1;
    c->file_size = filesize;
    c->file_received = 0;
    strncpy(c->file_name, filename, MAX_FILENAME - 1);
    strncpy(c->file_sender, c->username, MAX_USERNAME - 1);

    if (filesize > MAX_FILE_BYTES) {
        send_err(c, 4, "FILE_TOO_LARGE");
        c->file_discard = 1;   /* still drain the bytes to keep framing in sync */
        c->file_fp = NULL;
        return;
    }
    c->file_discard = 0;

    /* storage path: ./storage/<regno>/<sender_username>/<filename> */
    char dirpath[768];
    snprintf(dirpath, sizeof(dirpath), "%s/%s", STORAGE_BASE, c->username);
    ensure_dir(dirpath);
    char fullpath[1024];
    snprintf(fullpath, sizeof(fullpath), "%s/%s", dirpath, filename);
    c->file_fp = fopen(fullpath, "wb");
    if (!c->file_fp) {
        send_err(c, 500, "STORAGE_ERROR");
        c->file_discard = 1;
        return;
    }

    /* notify recipients up front with the header line */
    char hdr[MAX_LINE];
    snprintf(hdr, sizeof(hdr), "MSG FILE %s %s %ld", c->username, filename, filesize);
    for (int i = 0; i < c->recipient_count; i++)
        send_line(c->recipients[i], hdr);

    log_event("SENDFILE start from=%s target=%s file=%s size=%ld",
               c->username, target, filename, filesize);
}

static void handle_quit(Client *c) {
    send_ok(c, "BYE");
    log_event("QUIT user=%s fd=%d", c->username[0] ? c->username : "(unregistered)", c->fd);
    c->quit_pending = 1;
}

static void handle_command(Client *c, char *line) {
    trim_trailing_cr(line);
    if (line[0] == '\0') return;

    char cmd[32];
    int n;
    if (sscanf(line, "%31s%n", cmd, &n) != 1) {
        send_err(c, 999, "UNKNOWN_COMMAND");
        return;
    }
    char *args = line + n;
    while (*args == ' ') args++;

    if (c->username[0] == '\0' && strcmp(cmd, "REGISTER") != 0) {
        send_err(c, 5, "NOT_REGISTERED");
        return;
    }

    if (strcmp(cmd, "REGISTER") == 0)      handle_register(c, args);
    else if (strcmp(cmd, "LIST") == 0)     handle_list(c);
    else if (strcmp(cmd, "BCAST") == 0)    handle_bcast(c, args);
    else if (strcmp(cmd, "PMSG") == 0)     handle_pmsg(c, args);
    else if (strcmp(cmd, "JOIN") == 0)     handle_join(c, args);
    else if (strcmp(cmd, "LEAVE") == 0)    handle_leave(c, args);
    else if (strcmp(cmd, "ROOMS") == 0)    handle_rooms(c);
    else if (strcmp(cmd, "RMSG") == 0)     handle_rmsg(c, args);
    else if (strcmp(cmd, "SENDFILE") == 0) handle_sendfile(c, args);
    else if (strcmp(cmd, "QUIT") == 0)     handle_quit(c);
    else send_err(c, 999, "UNKNOWN_COMMAND");
}

/* consumes as much of c->buf as possible: lines in LINE mode, raw bytes in FILE mode */
static void process_client_buffer(Client *c) {
    for (;;) {
        if (c->in_file) {
            if (c->buf_len == 0) return;
            long remaining = c->file_size - c->file_received;
            long take = (long)c->buf_len < remaining ? (long)c->buf_len : remaining;

            if (!c->file_discard) {
                if (c->file_fp) fwrite(c->buf, 1, take, c->file_fp);
                for (int i = 0; i < c->recipient_count; i++)
                    writen(c->recipients[i], c->buf, take);
            }
            c->file_received += take;
            memmove(c->buf, c->buf + take, c->buf_len - take);
            c->buf_len -= take;

            if (c->file_received >= c->file_size) {
                if (c->file_fp) { fclose(c->file_fp); c->file_fp = NULL; }
                if (!c->file_discard) {
                    send_ok(c, "FILE_RECEIVED %s", c->file_name);
                    log_event("SENDFILE complete from=%s file=%s size=%ld",
                               c->file_sender, c->file_name, c->file_size);
                }
                c->in_file = 0;
                c->file_discard = 0;
            } else {
                return; /* need more bytes */
            }
        } else {
            char *nl = memchr(c->buf, '\n', c->buf_len);
            if (!nl) {
                if (c->buf_len > MAX_LINE) {
                    send_err(c, 400, "LINE_TOO_LONG");
                    c->buf_len = 0; /* drop corrupt input, resync */
                }
                return;
            }
            size_t linelen = nl - c->buf;
            char line[MAX_LINE + 1];
            size_t copylen = linelen < MAX_LINE ? linelen : MAX_LINE;
            memcpy(line, c->buf, copylen);
            line[copylen] = '\0';

            size_t consumed = linelen + 1;
            memmove(c->buf, c->buf + consumed, c->buf_len - consumed);
            c->buf_len -= consumed;

            handle_command(c, line);
            if (c->quit_pending) return; /* stop processing further input */
        }
    }
}

static void disconnect_client(Client *c) {
    if (c->username[0] != '\0') {
        remove_client_from_all_rooms(c->fd);
        char notice[160];
        snprintf(notice, sizeof(notice), "MSG PRESENCE LEAVE %s", c->username);
        broadcast_msg(notice, c->fd);
        log_event("DISCONNECT user=%s fd=%d", c->username, c->fd);
    } else {
        log_event("DISCONNECT fd=%d (unregistered)", c->fd);
    }
    if (c->file_fp) { fclose(c->file_fp); c->file_fp = NULL; }
    close(c->fd);
    c->active = 0;
    c->fd = -1;
    c->username[0] = '\0';
    c->buf_len = 0;
    c->in_file = 0;
    c->quit_pending = 0;
}

int main(int argc, char **argv) {
    int listenfd, connfd, maxfd, nready, i;
    fd_set rset, allset;
    struct sockaddr_in servaddr, cliaddr;
    socklen_t clilen;

    logfp = fopen(LOG_FILE, "a");
    if (!logfp) {
        perror("cannot open log file");
        exit(1);
    }

    mkdir("./storage", 0755);
    mkdir(STORAGE_BASE, 0755);

    for (i = 0; i < MAX_CLIENTS; i++) { clients[i].active = 0; clients[i].fd = -1; }
    for (i = 0; i < MAX_ROOMS; i++) rooms[i].active = 0;

    listenfd = socket(AF_INET, SOCK_STREAM, 0);
    if (listenfd < 0) { perror("socket"); exit(1); }

    int optval = 1;
    setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &optval, sizeof(optval));

    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = htonl(INADDR_ANY);
    servaddr.sin_port = htons(SERVER_PORT);

    if (bind(listenfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("bind"); exit(1);
    }
    if (listen(listenfd, 128) < 0) { perror("listen"); exit(1); }

    printf("NetMessenger server (reg %s) listening on port %d\n", REGNO, SERVER_PORT);
    log_event("SERVER START port=%d", SERVER_PORT);

    FD_ZERO(&allset);
    FD_SET(listenfd, &allset);
    maxfd = listenfd;

    for (;;) {
        rset = allset;
        nready = select(maxfd + 1, &rset, NULL, NULL, NULL);
        if (nready < 0) {
            if (errno == EINTR) continue;
            perror("select"); break;
        }

        if (FD_ISSET(listenfd, &rset)) {
            clilen = sizeof(cliaddr);
            connfd = accept(listenfd, (struct sockaddr *)&cliaddr, &clilen);
            if (connfd >= 0) {
                int slot = -1;
                for (i = 0; i < MAX_CLIENTS; i++) {
                    if (!clients[i].active) { slot = i; break; }
                }
                if (slot < 0) {
                    const char *busy = "ERR 500 SERVER_FULL NID:" NID_TAG "\n";
                    writen(connfd, busy, strlen(busy));
                    close(connfd);
                } else {
                    memset(&clients[slot], 0, sizeof(Client));
                    clients[slot].fd = connfd;
                    clients[slot].active = 1;
                    FD_SET(connfd, &allset);
                    if (connfd > maxfd) maxfd = connfd;
                    log_event("CONNECT fd=%d addr=%s", connfd, inet_ntoa(cliaddr.sin_addr));
                }
            }
        }

        for (i = 0; i < MAX_CLIENTS; i++) {
            if (!clients[i].active) continue;
            int fd = clients[i].fd;
            if (!FD_ISSET(fd, &rset)) continue;

            char tmp[4096];
            ssize_t n = recv(fd, tmp, sizeof(tmp), 0);
            if (n <= 0) {
                FD_CLR(fd, &allset);
                disconnect_client(&clients[i]);
                continue;
            }

            Client *c = &clients[i];
            if (c->buf_len + (size_t)n > CBUF_SIZE) {
                /* client is misbehaving/flooding; drop connection */
                send_err(c, 400, "BUFFER_OVERFLOW");
                FD_CLR(fd, &allset);
                disconnect_client(c);
                continue;
            }
            memcpy(c->buf + c->buf_len, tmp, n);
            c->buf_len += n;

            process_client_buffer(c);

            if (c->quit_pending) {
                FD_CLR(fd, &allset);
                disconnect_client(c);
            }
        }
    }

    fclose(logfp);
    close(listenfd);
    return 0;
}
