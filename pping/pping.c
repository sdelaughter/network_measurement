/*pping
Send pings with a poisson distribution delay interval
Positional arguments are:
    - Target IP address
    - Average expected packets/second
    - Total duration in seconds
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <math.h>
#include <time.h>
#include <pthread.h>
#include <stdatomic.h>

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <arpa/inet.h>


#define PACKET_SIZE   64
#define SEQ_TABLE_SIZE  65536
#define POST_SLEEP 1
#define QUIET 0

static int sock;
static pid_t pid;
static struct timespec start_ts;

static double sent_time[SEQ_TABLE_SIZE];
static pthread_mutex_t sent_mutex = PTHREAD_MUTEX_INITIALIZER;

static atomic_int sent_count = 0;
static atomic_int recv_count = 0;
static double rtt_min = -1.0, rtt_max = -1.0, rtt_sum = 0.0;
static double int_min = -1.0, int_max = -1.0, int_sum = 0.0;
// static pthread_mutex_t stats_mutex = PTHREAD_MUTEX_INITIALIZER;

static atomic_bool stop_receiver = 0;


static inline double timespec_to_msec(const struct timespec *ts) {
    return ts->tv_sec * 1000 + ts->tv_nsec / 1000000;
}

static inline double timespec_to_nsec(const struct timespec *ts) {
    return ts->tv_sec * 1e9 + ts->tv_nsec;
}

// The standard function for calculating Internet checksums
unsigned short checksum(unsigned short* ptr, int nbytes) {
	register long sum;
	unsigned short oddbyte;
	register short answer;

	sum=0;
	while(nbytes>1) {
		sum+=*ptr++;
		nbytes-=2;
	}
	if(nbytes==1) {
		oddbyte=0;
		*((u_char*)&oddbyte)=*(u_char*)ptr;
		sum+=oddbyte;
	}

	sum = (sum>>16)+(sum & 0xffff);
	sum = sum + (sum>>16);
	answer=(short)~sum;

	return(answer);
}

static struct timespec poisson_delay(double lambda) {
    double u;
    do {
        u = (double)rand() / ((double)RAND_MAX + 1.0);
    } while (u <= 0.0); // avoid log(0)
    double seconds = -log(u) / lambda;

    struct timespec ts;
    ts.tv_sec = (time_t)seconds;
    ts.tv_nsec = (long)((seconds - ts.tv_sec) * 1e9);
    return ts;
}

static double now_elapsed(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec - start_ts.tv_sec) + (ts.tv_nsec - start_ts.tv_nsec) / 1e9;
}

static void* receiver_thread(void* arg) {
    (void)arg;
    char buf[1024];

    while (!atomic_load(&stop_receiver)) {
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);

        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                              (struct sockaddr*)&from, &fromlen);
        if (n < 0) {
            if (errno == EINTR)
                continue;
            if (errno == EWOULDBLOCK || errno == EAGAIN)
                continue; // timeout, check stop flag and loop
            perror("recvfrom");
            continue;
        }

        double recv_time = now_elapsed();
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);

        // Make sure the packet is long enough, and get header pointers
        if ((size_t)n < sizeof(struct iphdr) + sizeof(struct icmphdr)) continue;
        struct iphdr* ip_hdr = (struct iphdr* )buf;
        int ip_hdr_len = ip_hdr->ihl * 4;
        if ((size_t)n < (size_t)ip_hdr_len + sizeof(struct icmphdr)) continue;
        struct icmphdr* icmp_hdr = (struct icmphdr*)(buf + ip_hdr_len);

        // Ignore anything other than an echo reply
        if (icmp_hdr->type != 0) continue;
        if (icmp_hdr->code != 0) continue;

        unsigned short id  = ntohs(icmp_hdr->un.echo.id);
        unsigned short seq = ntohs(icmp_hdr->un.echo.sequence);

        // Ignore packets from other PIDs
        if (id != (pid & 0xFFFF)) continue;

        pthread_mutex_lock(&sent_mutex);
        double st = sent_time[seq % SEQ_TABLE_SIZE];
        sent_time[seq % SEQ_TABLE_SIZE] = -1; // Reset value to -1 after reading in case the sequence number wraps
        pthread_mutex_unlock(&sent_mutex);

        char from_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &from.sin_addr, from_str, sizeof(from_str));

        int ttl = ip_hdr->ttl;

        if (st < 0.0) continue; // No matching send timestamp found

        double rtt_ms = (recv_time - st) * 1000.0;

        atomic_fetch_add(&recv_count, 1);
        // pthread_mutex_lock(&stats_mutex);
        if (rtt_min < 0.0 || rtt_ms < rtt_min) rtt_min = rtt_ms;
        if (rtt_max < 0.0 || rtt_ms > rtt_max) rtt_max = rtt_ms;
        rtt_sum += rtt_ms;
        // pthread_mutex_unlock(&stats_mutex);

        if (!QUIET) {
            printf("[%ld.%06ld] %lu bytes from %s: icmp_seq=%u ttl=%u time=%.3f ms\n",
                    (long)now.tv_sec, now.tv_nsec / 1000L, n-ip_hdr_len, from_str, seq, ttl, rtt_ms);
        }
    }

    return NULL;
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        fprintf(stderr, "Usage: %s <target_ip> <mean_rate_pkts_per_sec> <count>\n", argv[0]);
        fprintf(stderr, "Example: %s 8.8.8.8 5.0 20\n", argv[0]);
        return 1;
    }

    // Parse arguments
    const char* target_ip = argv[1];    // target IP address
    double lambda = atof(argv[2]);      // mean packets per second
    double duration = atof(argv[3]);    // duration in seconds

    if (lambda <= 0.0) {
        fprintf(stderr, "Rate must be positive\n");
        return 1;
    }

    sock = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    if (sock < 0) {
        perror("Failed to create socket, do you have root priviliges?");
		exit(1);
    }

    struct timeval tv = { .tv_sec = 0, .tv_usec = 200000 }; // 200 ms
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, target_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid target IP address: %s\n", target_ip);
        close(sock);
        return 1;
    }

    srand(time(NULL));

    pid = getpid();

    for (int i = 0; i < SEQ_TABLE_SIZE; i++)
        sent_time[i] = -1.0;

    pthread_t recv_tid;
    if (pthread_create(&recv_tid, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create");
        close(sock);
        return 1;
    }

    char packet[PACKET_SIZE];

    memset(packet, 0, sizeof(packet));
    struct icmphdr* icmph = (struct icmphdr*)packet;
    icmph->type = 8;
    icmph->code = 0;
    icmph->un.echo.id = htons((unsigned short)(pid & 0xFFFF));
    for (size_t i = sizeof(struct icmphdr); i < sizeof(packet); i++) {
        packet[i] = (char)(i & 0xFF);
    }

    clock_gettime(CLOCK_MONOTONIC, &start_ts);
    double elapsed = 0.0;
    double send_ts;
    int seq = 1;
    while (elapsed < duration) {
        icmph->un.echo.sequence = htons((unsigned short)seq);
        icmph->checksum = 0;
        icmph->checksum = checksum((unsigned short*)packet, sizeof(packet));

        send_ts = now_elapsed();
        pthread_mutex_lock(&sent_mutex);
        sent_time[seq % SEQ_TABLE_SIZE] = send_ts;
        pthread_mutex_unlock(&sent_mutex);

        ssize_t sent = sendto(sock, packet, sizeof(packet), 0,
                                (struct sockaddr*)&addr, sizeof(addr));
        if (sent < 0) perror("sendto");
        else atomic_fetch_add(&sent_count, 1);

        struct timespec ts = poisson_delay(lambda);
        nanosleep(&ts, NULL);

        double int_ms = timespec_to_msec(&ts);
        if (int_min < 0.0 || int_ms < int_min) int_min = int_ms;
        if (int_max < 0.0 || int_ms > int_max) int_max = int_ms;
        int_sum += int_ms;

        seq += 1;
        elapsed = now_elapsed();
    }

    sleep(POST_SLEEP);

    atomic_store(&stop_receiver, 1);
    pthread_join(recv_tid, NULL);

    int n_sent = atomic_load(&sent_count);
    int n_recv = atomic_load(&recv_count);
    double loss_pct = 0.0;
    if (n_sent > 0) {
        loss_pct = ((n_sent - n_recv) / n_sent) * 100.0;
    }

    printf("\n--- %s pping statistics ---\n", target_ip);

    double total_duration = now_elapsed() * 1000;

    printf("%d packets transmitted, %d received, %.1f%% packet loss, time %ums\n", n_sent, n_recv, loss_pct, (int)total_duration);
    if (n_recv > 0) {
        printf("rtt min/avg/max = %.3f/%.3f/%.3f ms\n",
            rtt_min, rtt_sum / n_recv, rtt_max);
        printf("interval min/avg/max = %.3f/%.3f/%.3f ms\n",
            int_min, int_sum / n_recv, int_max);
        printf("pps avg = %.3f\n",
            n_recv / duration);
    }

    close(sock);
    return 0;
}
