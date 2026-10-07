/*
 * client_6572.c
 * NetMessenger client - IE3010 Network Programming Assignment
 * Registration number: IT23816572
 *
 * Usage: ./client_6572.out <server-ip> [port]
 * Then type protocol commands, e.g.:
 *   REGISTER alice
 *   BCAST hello everyone
 *   PMSG bob hi there
 *   JOIN general
 *   RMSG general hello room
 *   SENDFILE bob myfile.txt         (filesize is read from the local file automatically)
 *   QUIT
 *
 * Incoming files from other clients are saved under ./received_6572/<filename>.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/select.h>
#include <sys/stat.h>

#define DEFAULT_PORT   12572
#define MAX_LINE       1024
#define CBUF_SIZE      65536
#define RECV_DIR       "./received_6572"

static char   rbuf[CBUF_SIZE];
static size_t rbuf_len = 0;

static int    in_file = 0;
static long   file_size = 0, file_received = 0;
static char   file_name[256];
static FILE  *file_fp = NULL;

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

static void ensure_recv_dir(void) { mkdir(RECV_DIR, 0755); }

static void start_incoming_file(const char *sender, const char *fname, long fsize) {
    (void)sender;
    ensure_recv_dir();
    char path[600];
    snprintf(path, sizeof(path), "%s/%s", RECV_DIR, fname);
    file_fp = fopen(path, "wb");
    strncpy(file_name, fname, sizeof(file_name) - 1);
    file_size = fsize;
    file_received = 0;
    in_file = 1;
    printf("[receiving file \"%s\" (%ld bytes) -> %s]\n", fname, fsize, path);
}

static void handle_server_line(char *line) {
    size_t n = strlen(line);
    if (n > 0 && line[n - 1] == '\r') line[n - 1] = '\0';

    if (strncmp(line, "MSG FILE ", 9) == 0) {
        char sender[128], fname[256];
        long fsize;
        if (sscanf(line + 9, "%127s %255s %ld", sender, fname, &fsize) == 3) {
            start_incoming_file(sender, fname, fsize);
            return;
        }
    }
    printf("%s\n", line);
    fflush(stdout);
}

/* consumes as much of rbuf as possible: lines normally, raw bytes while in_file */
static void process_server_buffer(void) {
    for (;;) {
        if (in_file) {
            if (rbuf_len == 0) return;
            long remaining = file_size - file_received;
            long take = (long)rbuf_len < remaining ? (long)rbuf_len : remaining;
            if (file_fp) fwrite(rbuf, 1, take, file_fp);
            file_received += take;
            memmove(rbuf, rbuf + take, rbuf_len - take);
            rbuf_len -= take;
            if (file_received >= file_size) {
                if (file_fp) { fclose(file_fp); file_fp = NULL; }
                printf("[file \"%s\" received complete]\n", file_name);
                fflush(stdout);
                in_file = 0;
            } else {
                return;
            }
        } else {
            char *nl = memchr(rbuf, '\n', rbuf_len);
            if (!nl) {
                if (rbuf_len > MAX_LINE) rbuf_len = 0; /* resync on garbage */
                return;
            }
            size_t linelen = nl - rbuf;
            char line[MAX_LINE + 1];
            size_t copylen = linelen < MAX_LINE ? linelen : MAX_LINE;
            memcpy(line, rbuf, copylen);
            line[copylen] = '\0';
            size_t consumed = linelen + 1;
            memmove(rbuf, rbuf + consumed, rbuf_len - consumed);
            rbuf_len -= consumed;
            handle_server_line(line);
        }
    }
}

/* Handle a SENDFILE typed by the local user: read local file, send header + bytes */
static void do_local_sendfile(int sockfd, char *args) {
    char target[128], localpath[512];
    if (sscanf(args, "%127s %511s", target, localpath) != 2) {
        printf("usage: SENDFILE <target> <local-file-path>\n");
        return;
    }
    FILE *fp = fopen(localpath, "rb");
    if (!fp) {
        printf("cannot open local file: %s\n", localpath);
        return;
    }
    fseek(fp, 0, SEEK_END);
    long fsize = ftell(fp);
    fseek(fp, 0, SEEK_SET);

    /* filename sent in the protocol is just the base name */
    const char *base = strrchr(localpath, '/');
    base = base ? base + 1 : localpath;

    char header[700];
    snprintf(header, sizeof(header), "SENDFILE %s %s %ld\n", target, base, fsize);
    writen(sockfd, header, strlen(header));

    char buf[4096];
    size_t r;
    while ((r = fread(buf, 1, sizeof(buf), fp)) > 0) {
        if (writen(sockfd, buf, r) < 0) break;
    }
    fclose(fp);
    printf("[sent file \"%s\" (%ld bytes) to %s]\n", base, fsize, target);
}

int main(int argc, char **argv) {
    if (argc < 2) {
        fprintf(stderr, "usage: %s <server-ip> [port]\n", argv[0]);
        exit(1);
    }
    int port = (argc >= 3) ? atoi(argv[2]) : DEFAULT_PORT;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); exit(1); }

    struct sockaddr_in servaddr;
    memset(&servaddr, 0, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    if (inet_pton(AF_INET, argv[1], &servaddr.sin_addr) <= 0) {
        fprintf(stderr, "invalid address: %s\n", argv[1]);
        exit(1);
    }
    if (connect(sockfd, (struct sockaddr *)&servaddr, sizeof(servaddr)) < 0) {
        perror("connect"); exit(1);
    }
    printf("connected to %s:%d\n", argv[1], port);

    fd_set rset;
    int maxfd = (sockfd > STDIN_FILENO ? sockfd : STDIN_FILENO) + 1;
    int stdin_open = 1;
    int quit_sent = 0;

    for (;;) {
        FD_ZERO(&rset);
        if (stdin_open) FD_SET(STDIN_FILENO, &rset);
        FD_SET(sockfd, &rset);

        int nready = select(maxfd, &rset, NULL, NULL, NULL);
        if (nready < 0) {
            if (errno == EINTR) continue;
            perror("select"); break;
        }

        if (FD_ISSET(sockfd, &rset)) {
            char tmp[4096];
            ssize_t n = recv(sockfd, tmp, sizeof(tmp), 0);
            if (n <= 0) {
                printf("[server closed the connection]\n");
                break;
            }
            if (rbuf_len + (size_t)n > CBUF_SIZE) rbuf_len = 0; /* safety reset */
            memcpy(rbuf + rbuf_len, tmp, n);
            rbuf_len += n;
            process_server_buffer();
            if (quit_sent && !stdin_open) break; /* drained reply to our QUIT */
        }

        if (stdin_open && FD_ISSET(STDIN_FILENO, &rset)) {
            char line[MAX_LINE];
            if (fgets(line, sizeof(line), stdin) == NULL) {
                /* EOF on stdin: send QUIT, stop reading stdin, wait for OK BYE */
                writen(sockfd, "QUIT\n", 5);
                stdin_open = 0;
                quit_sent = 1;
                continue;
            }
            size_t ln = strlen(line);
            if (ln > 0 && line[ln - 1] == '\n') line[ln - 1] = '\0';

            if (strncmp(line, "SENDFILE ", 9) == 0) {
                do_local_sendfile(sockfd, line + 9);
            } else {
                char out[MAX_LINE + 1];
                snprintf(out, sizeof(out), "%s\n", line);
                writen(sockfd, out, strlen(out));
                if (strcmp(line, "QUIT") == 0) {
                    stdin_open = 0;
                    quit_sent = 1;
                }
            }
        }
    }

    close(sockfd);
    return 0;
}
