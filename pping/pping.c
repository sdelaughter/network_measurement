/* pping (Poisson Ping)
Author: Sam DeLaughter

Send ICMP Echo Requests at a rate that follows a Poisson distribution.
Prints output that (mostly) matches that of the traditional ping command, plus some extra statistics.
Also supports JSON-formatted output (without summary statistics).

For usage information, try `pping -h` or read the `help_string` below.

Compile with:
gcc -O2 -Wall -o pping pping.c -lm
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
#include <signal.h>

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <arpa/inet.h>

#define SEQ_TABLE_SIZE  65536   // Max number of sequence/timestamp mappings to store before wrapping
#define DRY_RUN 0               // Print argument values and exit, for debugging purposes

// Define help string
const char* help_string = "\n\
Usage\n\
    pping [options] <destination>\n\
\n\
Options:\n\
    <destination>       Destination IP address\n\
    -c                  Number of packets to send, unless duration is reached first. Default: unlimited.\n\
    -i                  Average interval in seconds between packets. Mutually exclusive with -r. Default: 1.\n\
    -I                  Specify the name of a network interface to bind to.\n\
    -j                  Enable JSON-formatted output.\n\
    -q                  Enable quiet mode, to print only summary statistics with no per-packet output.\n\
    -r                  Average number of packets per second.  Mutually exclusive with -i. Default: 1.\n\
    -s                  Size of ICMP payload to send.  Additional 8-byte ICMP header will be added. Default: 56.\n\
    -w                  Duration in seconds to send for, unless count is reached first.  Default: unlimited.\n\
    -W                  Time in seconds to wait for replies after last packet is sent.  Default: 1.\n\
";

// Set default values for command-line arguments
static char*    target_ip   = "127.0.0.1";
static char*    bind_ifname = NULL;
static int      quiet       = 0;
static int      packet_size = 64;
static double   lambda      = 1.0;
static int      count       = -1;
static double   duration    = -1.0;
static double   timeout     = 1.0;
static int      json        = 0;

// Initialize other static variables
static int sock;
static struct sockaddr_in addr;
static pid_t pid;
static struct timespec start_ts;
static double sent_time[SEQ_TABLE_SIZE];
static pthread_mutex_t sent_mutex = PTHREAD_MUTEX_INITIALIZER;
static atomic_int sent_count = 0;
static atomic_int recv_count = 0;
static double rtt_min = -1.0, rtt_max = -1.0, rtt_sum = 0.0;
static double int_min = -1.0, int_max = -1.0, int_sum = 0.0;
static atomic_bool stop_sender = 0;
static atomic_bool stop_receiver = 0;

// Helpers for time conversion
static inline double timespec_to_msec(const struct timespec *ts) {
    return ts->tv_sec * 1000 + ts->tv_nsec / 1000000;
}
static inline double timespec_to_nsec(const struct timespec *ts) {
    return ts->tv_sec * 1e9 + ts->tv_nsec;
}

// Catch ctrl-C signal and stop the sender
void interrupt_handler(int i) {
    atomic_store(&stop_sender, 1);
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

// Determine a random amount of time to wait based on a given lambda
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

// Compute the amount of time elapsed since the program started
static double now_elapsed(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (ts.tv_sec - start_ts.tv_sec) + (ts.tv_nsec - start_ts.tv_nsec) / 1e9;
}

// Parse command-line arguments
void parse_args(int argc, char* argv[]) {
    int got_interval_arg = 0, got_rate_arg = 0;
    int opt;
    while ((opt = getopt(argc, argv, "c:hi:I:jqr:s:w:W:")) != -1) {
        switch (opt) {
            case 'c':
                count = atoi(optarg);
                break;
            case 'i':
                lambda = 1.0/atof(optarg);
                got_interval_arg = 1;
                break;
            case 'I':
                bind_ifname = optarg;
                break;
            case 'j':
                json = 1;
                break;
            case 'q':
                quiet = 1;
                break;
            case 'r':
                lambda = atof(optarg);
                got_rate_arg = 1;
                break;
            case 's':
                packet_size = atoi(optarg) + 8; // 8 byte ICMP header
                break;
            case 'w':
                duration = atof(optarg);
                break;
            case 'W':
                timeout = atof(optarg);
                break;
            case 'h':
                printf("%s", help_string);
                exit(0);
            default:
                printf("%s", help_string);
                exit(1);
        }
    }
    if (optind < argc) target_ip = argv[optind];

    // Make sure we don't have both -i and -r arguments
    if (got_interval_arg && got_rate_arg) {
        fprintf(stderr, "The -i (interval) and -r (rate) arguments are mutually exclusive.  You must use one or the other, not both.\n");
        exit(1);
    }

    // Make sure we don't have both -j and -q arguments
    if (json && quiet) {
        fprintf(stderr, "The -j (json) and -q (quiet) arguments are mutually exclusive.  You must use one or the other, not both.\n");
        exit(1);
    }

    // Make sure the target sending rate is positive
    if (lambda <= 0.0) {
        if (got_interval_arg) fprintf(stderr, "Interval must be positive\n");
        else fprintf(stderr, "Rate must be positive\n");
        exit(1);
    }

    // Make sure the destination is a valid IPv4 address
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, target_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid target IP address: %s\n", target_ip);
        exit(1);
    }
    
    #if DRY_RUN
        // Display arguments and exit, for debugging purposes
        printf("Arguments are:\n\
            Target IP: %s\n\
            Interface: %s\n\
            Count: %d\n\
            Quiet: %u\n\
            JSON: %u\n\
            Lambda: %f\n\
            Size: %u\n\
            Duration: %f\n\
            Timeout: %f\n",
            target_ip, bind_ifname, count, quiet, json, lambda, packet_size, duration, timeout
        );
        exit(0);
    #endif
}

// Listen for echo reply packets
static void* receiver_thread(void* arg) {
    (void)arg;
    char buf[1024];
    struct sockaddr_in from;
    socklen_t fromlen = sizeof(from);

    while (!atomic_load(&stop_receiver)) {
        // Receive a packet
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                              (struct sockaddr*)&from, &fromlen);
        if (n < 0) {
            if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) continue;
            perror("recvfrom");
            continue;
        }

        // Compute time since start and current timestamp
        // TODO: Combine these to use a single clock_gettime call
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

        // Ignore packets from other PIDs
        unsigned short id  = ntohs(icmp_hdr->un.echo.id);
        if (id != (pid & 0xFFFF)) continue;

        // Retrieve the sending timestamp for this sequence number
        unsigned short seq = ntohs(icmp_hdr->un.echo.sequence);
        pthread_mutex_lock(&sent_mutex);
        double st = sent_time[seq % SEQ_TABLE_SIZE];
        sent_time[seq % SEQ_TABLE_SIZE] = -1; // Reset value to -1 after reading in case the sequence number wraps
        pthread_mutex_unlock(&sent_mutex);
        if (st < 0.0) continue; // No matching send timestamp found

        // Get the source address and TTL from the reply
        char from_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &from.sin_addr, from_str, sizeof(from_str));
        int ttl = ip_hdr->ttl;

        // Compute the RTT and update statistics
        double rtt_ms = (recv_time - st) * 1000.0;
        atomic_fetch_add(&recv_count, 1);
        if (rtt_min < 0.0 || rtt_ms < rtt_min) rtt_min = rtt_ms;
        if (rtt_max < 0.0 || rtt_ms > rtt_max) rtt_max = rtt_ms;
        rtt_sum += rtt_ms;

        // Print per-packet output
        if (!quiet) {
            if (json) {
                if (seq > 1) printf(",\n");
                printf("\
    {\n\
        \"timestamp\": %ld.%06ld,\n\
        \"bytes\": %lu,\n\
        \"from\": \"%s\",\n\
        \"icmp_seq\": %u,\n\
        \"ttl\": %u,\n\
        \"rtt\": %.3f\n\
    }", (long)now.tv_sec, now.tv_nsec / 1000L, n-ip_hdr_len, from_str, seq, ttl, rtt_ms);
            } else {
                printf("[%ld.%06ld] %lu bytes from %s: icmp_seq=%u ttl=%u time=%.3f ms\n",
                        (long)now.tv_sec, now.tv_nsec / 1000L, n-ip_hdr_len, from_str, seq, ttl, rtt_ms);
            }
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    srand(time(NULL));
    pid = getpid();

    // Prepare to handle interrupts
    struct sigaction act;
    bzero(&act, sizeof(act));
    act.sa_handler = &interrupt_handler;
    sigaction(SIGINT, &act, NULL);

    // Parse command-line arguments (results will be stored in static variables)
    parse_args(argc, argv);

    // Create a socket
    sock = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    if (sock < 0) {
        fprintf(stderr, "Failed to create socket, do you have root priviliges?\n");
		exit(1);
    }

    int timeout_usec = timeout * 1000000;
    int timeout_sec = floor(timeout_usec / 1000000);
    timeout_usec = timeout_usec % 1000000;
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = timeout_usec;
    setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));

    if (bind_ifname != NULL) {
        if (setsockopt(sock, SOL_SOCKET, SO_BINDTODEVICE, bind_ifname, strlen(bind_ifname) + 1) < 0) {
            fprintf(stderr, "Failed to bind to device with name '%s'.  Make sure the interface exists and you have root priviliges.\n", bind_ifname);
            close(sock);
            exit(1);
        }
    }

    // Initialize array to store timestamps
    for (int i=0; i<SEQ_TABLE_SIZE; i++) {
        sent_time[i] = -1.0;
    }

    // Start receiver thread to listen for Echo Reply packets
    pthread_t recv_tid;
    if (pthread_create(&recv_tid, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create");
        close(sock);
        exit(1);
    }

    // Create ICMP packet
    char packet[packet_size];
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

    
    if (json) printf("[\n");
    else printf("PPING %s (%s) %u(%u) bytes of data.\n", target_ip, target_ip, packet_size-8, packet_size+20);

    // Start sending packets.  Continue until count/duration is exceeded or Ctrl-C is pressed
    while ((elapsed < duration || duration < 0) && (seq <= count || count < 0) && !atomic_load(&stop_sender)) {
        // Update sequence number and recompute checksum
        icmph->un.echo.sequence = htons((unsigned short)seq);
        icmph->checksum = 0;
        icmph->checksum = checksum((unsigned short*)packet, sizeof(packet));

        // Compute timestamp relative to start time and store it for later
        send_ts = now_elapsed();
        pthread_mutex_lock(&sent_mutex);
        sent_time[seq % SEQ_TABLE_SIZE] = send_ts;
        pthread_mutex_unlock(&sent_mutex);

        // Send packet
        ssize_t sent = sendto(sock, packet, sizeof(packet), 0,
                                (struct sockaddr*)&addr, sizeof(addr));
        if (sent < 0) perror("sendto");
        else atomic_fetch_add(&sent_count, 1);

        // Wait for some amount of time determined by Poisson distribution
        seq += 1;
        if ((seq <= count || count < 0) && !atomic_load(&stop_sender)) {
            struct timespec ts = poisson_delay(lambda);
            nanosleep(&ts, NULL);
            elapsed = now_elapsed();

            if (elapsed < duration || duration < 0) {
                // Update interval statistics
                double int_ms = timespec_to_msec(&ts);
                if (int_min < 0.0 || int_ms < int_min) int_min = int_ms;
                if (int_max < 0.0 || int_ms > int_max) int_max = int_ms;
                int_sum += int_ms;
            }
        }
    }

    // Wait for replies to arrive before stopping receiver
    sleep(timeout);
    atomic_store(&stop_receiver, 1);
    pthread_join(recv_tid, NULL);

    // Compute and print summary statistics
    if (json) {
        printf("\n]\n");
    } else {
        int n_sent = atomic_load(&sent_count);
        int n_recv = atomic_load(&recv_count);
        double loss_pct = 0.0;
        if (n_sent > 0) {
            loss_pct = ((n_sent - n_recv) / n_sent) * 100.0;
        }
        printf("\n--- %s pping statistics ---\n", target_ip);
        double total_duration = (now_elapsed()-timeout);
        printf("%d packets transmitted, %d received, %.1f%% packet loss, time %ums\n", n_sent, n_recv, loss_pct, (int)(total_duration*1000.0));
        
        // RTT statistics require at least one packet received
        if (n_recv > 0) {
            printf("rtt min/avg/max = %.3f/%.3f/%.3f ms\n",
                rtt_min, rtt_sum / n_recv, rtt_max);
        }

        // Statistics about inter-packet intervals require at least two packets sent
        if (n_sent > 1) {
            printf("interval min/avg/max = %.3f/%.3f/%.3f ms\n",
                int_min, int_sum / n_sent, int_max);
            printf("pps avg = %.3f\n",
                n_sent / total_duration);
        } else {
            printf("interval min/avg/max = -1/-1/-1 ms\n");
            printf("pps avg = -1\n"); 
        }
    }

    // Close the socket and exit
    close(sock);
    return 0;
}
