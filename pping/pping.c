/* pping (Precise/Probabilistic Ping)
Author: Sam DeLaughter

Send ICMP Echo Requests at a rate that follows a Poisson or Uniform distribution.
Prints output that (mostly) matches that of the traditional ping command, plus some extra statistics.
Also supports JSON-formatted output (without summary statistics).
For a fixed inter-packet interval, provides more accuracy for fine-grained interval adjustment than other implementations.

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
#include <float.h>

#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <netinet/ip_icmp.h>
#include <arpa/inet.h>

#define SEQ_TABLE_SIZE  65536   // Max number of sequence/timestamp mappings to store before wrapping
#define DRY_RUN 0               // Print argument values and exit, for debugging purposes
#define VERSION "0.1"

#define ICMP_TYPE_ECHO_REQUEST 8
#define ICMP_TYPE_ECHO_REPLY 0
#define ICMP_TYPE_TTL_EXCEEDED 11

#pragma region Usage
// Define help string
const char* help_string = "\n\
Usage\n\
    pping [options] <destination>\n\
\n\
Options:\n\
    <destination>       Destination IP address\n\
    -c <count>          Stop after sending <count> packets. Default: unlimited.\n\
    -d                  Set the SO_DEBUG option on the socket being used.\n\
    -g <group_size>     Send packets in bursts of <group_size> at a time before waiting for the next delay interval.  Default: 1.\n\
    -h                  Show this help message and exit.\n\
    -i <interval>       Wait <interval> seconds between packets (on average, if using -P or -u). Mutually exclusive with -r. Default: 1.\n\
    -I <interface>      Bind to the network interface with name <interface>.\n\
    -j                  Enable JSON-formatted output.\n\
    -o <offset>         Time packets so that the fractional portion of their timestamps are the same as that of <offset> (in seconds).\n\
    -P                  Use a Poisson distribution insted of a fixed interval delay between packets.\n\
    -q                  Enable quiet mode, to print only summary statistics with no per-packet output.\n\
    -r <rate>           Send <rate> packets per second (on average, if using -P or -u). Mutually exclusive with -i. Default: 1.\n\
    -s <size>           Send ICMP payloads with <size> bytes.  An additional 8-byte ICMP header will be added. Default: 56.\n\
    -t <TTL>            Set the IP time-to-live field to <TTL>.\n\
    -u <uniform_range>  Send packets at intervals following a uniform distribution with width <uniform_range> around the target interval set by -i/-r.\n\
    -V                  Print the version number and exit.\n\
    -w <deadline>       Stop sending after <deadline> seconds.  Default: unlimited.\n\
    -W <timeout>        Wait <timeout> seconds for replies after the last packet is sent.  Default: 1.\n\
    -x <max_interval>   Wait at most <maximum_interval> seconds between packets.  Enforced by setting any would-be longer delays to instead be this value.  Default: none.\n\
    -X <max_interval>   Wait at most <maximum_interval> seconds between packets.  Enforced by halving any would-be longer delays until they are <= this value.  Default: none.\n\
";

#pragma endregion Usage


#pragma region Static_Variables
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
static double   max_delay   = -1;
static double   max_delay_2 = -1;
static int      sock_debug  = 0;
static int      do_poisson = 0;
static double   uniform_range = -1.0;
static int      group_size = 1;
static int      set_ttl = -1;
static double   offset_seconds = -1.0;

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
#pragma endregion Static_Variables


#pragma region Control_Flow

// Catch ctrl-C signal and stop the sender
static void interrupt_handler(int i) {
    atomic_store(&stop_sender, 1);
}

static void start_interrupt_handler(void) {
    struct sigaction act;
    bzero(&act, sizeof(act));
    act.sa_handler = &interrupt_handler;
    sigaction(SIGINT, &act, NULL);
}

#pragma endregion Control_Flow


#pragma region Time_Helpers
inline double timespec_to_sec(const struct timespec *ts) {
    return (double)ts->tv_sec + (double)ts->tv_nsec / 1e9;
}

inline double timespec_to_msec(const struct timespec *ts) {
    return (double)ts->tv_sec * 1e3 + (double)ts->tv_nsec / 1e6;
}

inline double timespec_to_nsec(const struct timespec *ts) {
    return ts->tv_sec * 1e9 + ts->tv_nsec;
}

struct timespec sec_to_timespec(double seconds) {
    struct timespec ts;
    ts.tv_sec = (time_t)seconds;
    ts.tv_nsec = (long)((seconds - ts.tv_sec) * 1e9);
    if (ts.tv_nsec >= 1000000000L) {
        ts.tv_sec++;
        ts.tv_nsec -= 1000000000L;
    }
    return ts;
}

inline struct timespec msec_to_timespec(int64_t msec) {
    struct timespec ts;
    ts.tv_sec  = (time_t)(msec / 1000);
    ts.tv_nsec = (long)((msec % 1000) * 1000000LL);
    return ts;
}

inline struct timespec nsec_to_timespec(int64_t nsec) {
    struct timespec ts;
    ts.tv_sec  = (time_t)(nsec / 1000000000LL);
    ts.tv_nsec = (long)(nsec % 1000000000LL);
    return ts;
}

// Get the current time
inline void current_time(struct timespec* ts) {
    clock_gettime(CLOCK_REALTIME, ts);
}

// Compute the difference between two timespecs in seconds
inline double time_diff(struct timespec start, struct timespec stop) {
    return (stop.tv_sec - start.tv_sec) + ((stop.tv_nsec - start.tv_nsec) / 1e9);
}

// Compute the amount of time elapsed since the program started
static inline double now_elapsed(void) {
    struct timespec now;
    current_time(&now);
    return time_diff(start_ts, now);
}
#pragma endregion Time_Helpers


#pragma region Network_Helpers
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

inline int packet_is_icmp(const struct iphdr* ip_hdr) {
    return (ip_hdr->protocol == IPPROTO_ICMP);
}

inline int icmp_is_echo_request(const struct icmphdr* icmph) {
    return ((icmph->type == ICMP_TYPE_ECHO_REQUEST) && (icmph->code == 0));
}

inline int icmp_is_echo_reply(const struct icmphdr* icmph) {
    return ((icmph->type == ICMP_TYPE_ECHO_REPLY) && (icmph->code == 0));
}

inline int icmp_is_ttl_exceeded(const struct icmphdr* icmph) {
    return ((icmph->type == ICMP_TYPE_TTL_EXCEEDED) && (icmph->code == 0));
}

#pragma endregion Network_Helpers


#pragma region Socket_Configuration
static int set_socket_ttl(int s, int ttl) {
    return setsockopt(s, IPPROTO_IP, IP_TTL, &ttl, sizeof(ttl));
}

static int set_socket_timeout(int s, double duration) {
    int timeout_usec = duration * 1000000;
    int timeout_sec = floor(timeout_usec / 1000000);
    timeout_usec = timeout_usec % 1000000;
    struct timeval tv;
    tv.tv_sec = timeout_sec;
    tv.tv_usec = timeout_usec;
    return setsockopt(s, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

static int set_socket_debug(int s, int enable) {
    return setsockopt(s, SOL_SOCKET, SO_DEBUG, (char *)&enable, sizeof(enable));
}

static int set_socket_bind_ifname(int s, char* ifname) {
    return setsockopt(sock, SOL_SOCKET, SO_BINDTODEVICE, ifname, strlen(ifname) + 1);
}

#pragma endregion Socket_Options


#pragma region Delay_Computation

static void poisson_delay(struct timespec* ts, double lambda) {
    double u;
    do {
        u = (double)rand() / ((double)RAND_MAX + 1.0);
    } while (u <= 0.0); // avoid log(0)
    double seconds = -log(u) / lambda;

    if (max_delay >= 0) {
        seconds = fmin(seconds, max_delay);
    } else if (max_delay_2 > 0) {
        while (seconds > max_delay_2) {
            seconds = seconds / 2.0;
        }
    }

    *ts = sec_to_timespec(seconds);
}

static void uniform_delay(struct timespec* ts, double lambda, double range) {
    double  center    = 1.0 / lambda;
    int64_t center_ns = (int64_t)(center * 1e9);
    int64_t range_ns  = (int64_t)(range * 1e9);
    int64_t offset_ns = (int64_t)((double)rand() / ((double)RAND_MAX + 1.0) * range_ns) - (range_ns/2);
    int64_t total_ns  = center_ns + offset_ns;

    *ts = nsec_to_timespec(total_ns);
}

static void fixed_delay(struct timespec* ts, double lambda) {
    double seconds = 1.0/lambda;
    ts->tv_sec = (time_t)seconds;
    ts->tv_nsec = (long)((seconds - ts->tv_sec) * 1e9);
}

static void offset_delay(struct timespec* ts, double offset) {
    struct timespec now;
    current_time(&now);
    double now_seconds = timespec_to_sec(&now);
    double diff = fmod(offset - now_seconds, 1.0);
    if (diff < 0.0)
        diff += 1.0;
    *ts = sec_to_timespec(diff);
}

static void zero_delay(struct timespec* ts) {
    ts->tv_sec = (time_t)0;
    ts->tv_nsec = (long)0;
}

#pragma endregion Delay_Functions


#pragma region Misc_Helpers

 // Retrieve the sending timestamp for a given sequence number
static double get_sent_timestamp(unsigned short seq) {
    pthread_mutex_lock(&sent_mutex);
    double st = sent_time[seq % SEQ_TABLE_SIZE];
    sent_time[seq % SEQ_TABLE_SIZE] = -1; // Reset value to -1 after reading in case the sequence number wraps
    pthread_mutex_unlock(&sent_mutex);
    return st;
}

static void set_sent_timestamp(unsigned short seq, double ts) {
    pthread_mutex_lock(&sent_mutex);
    sent_time[seq % SEQ_TABLE_SIZE] = ts;
    pthread_mutex_unlock(&sent_mutex);
}

static inline int same_process(const struct icmphdr* icmph) {
    unsigned short id  = ntohs(icmph->un.echo.id);
    return (id == (pid & 0xFFFF));
}

static void print_packet_json(const struct timespec* now, unsigned long bytes, char* from_str, int seq, int ttl, double rtt_ms, char* error) {
    if (seq > 1) printf(",\n");
    printf("\
{\n\
    \"timestamp\": %ld.%06ld,\n\
    \"bytes\": %lu,\n\
    \"from\": \"%s\",\n\
    \"icmp_seq\": %u,\n\
    \"ttl\": %u,\n\
    \"rtt\": %.3f,\n\
    \"err\": \"%s\"\n\
}", (long)now->tv_sec, now->tv_nsec / 1000L, bytes, from_str, seq, ttl, rtt_ms, error);
}

static void print_packet_info(const struct timespec* now, unsigned long bytes, char* from_str, int seq, int ttl, double rtt_ms, int type) {
    if (quiet) return;
    if (type == ICMP_TYPE_ECHO_REPLY) {
        if (json) {
            print_packet_json(now, bytes, from_str, seq, ttl, rtt_ms, "");
        } else {
            printf("[%ld.%06ld] %lu bytes from %s: icmp_seq=%u ttl=%u time=%.3f ms\n",
                    (long)now->tv_sec, now->tv_nsec / 1000L, bytes, from_str, seq, ttl, rtt_ms
            );
        }
    } else if (type == ICMP_TYPE_TTL_EXCEEDED) {
        if (json) {
            print_packet_json(now, bytes, from_str, seq, ttl, rtt_ms, "TTL Exceeded");
        } else {
            printf("[%ld.%06ld] From %s icmp_seq=%u Time to live exceeded after %.3f ms\n", 
                (long)now->tv_sec, now->tv_nsec / 1000L, from_str, seq, rtt_ms
            );
        }
    }
}

#pragma endregion Misc_Helpers


// Parse command-line arguments
void parse_args(int argc, char* argv[]) {
    int got_interval_arg = 0, got_rate_arg = 0; // For exclusivity check
    int got_max_delay = 0, got_max_delay_2 = 0; // For exclusivity check
    int opt;
    while ((opt = getopt(argc, argv, "c:dg:hi:I:jo:Pqr:s:t:u:Vw:W:x:X:")) != -1) {
        switch (opt) {
            case 'c':
                count = atoi(optarg);
                break;
            case 'd':
                sock_debug = 1;
                break;
            case 'g':
                group_size = atoi(optarg);
                break;
            case 'h':
                printf("%s", help_string);
                exit(0);
            case 'i':
                double interval = atof(optarg);
                if (interval <= 0) {
                    lambda = -1;
                } else {
                    lambda = 1.0/atof(optarg);
                }
                got_interval_arg = 1;
                break;
            case 'I':
                bind_ifname = optarg;
                break;
            case 'j':
                json = 1;
                break;
            case 'o':
                offset_seconds = atof(optarg);
                break;
            case 'P':
                do_poisson = 1;
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
            case 't':
                set_ttl = atoi(optarg);
                break;
            case 'u':
                uniform_range = atof(optarg);
                break;
            case 'V':
                printf("pping v%s\n", VERSION);
                exit(0);
                break;
            case 'w':
                duration = atof(optarg);
                break;
            case 'W':
                timeout = atof(optarg);
                break;
            case 'x':
                max_delay = atof(optarg);
                got_max_delay = 1;
                break;
            case 'X':
                max_delay_2 = atof(optarg);
                got_max_delay_2 = 1;
                break;
            default:
                printf("%s", help_string);
                exit(2);
        }
    }
    if (optind < argc) target_ip = argv[optind];

    // Make sure we don't have both -i and -r arguments
    if (got_interval_arg && got_rate_arg) {
        fprintf(stderr, "The -i (interval) and -r (rate) arguments are mutually exclusive.  You must use one or the other, not both.\n");
        exit(2);
    }

    // Make sure we don't have both -P and -u arguments
    if (do_poisson && (uniform_range >= 0.0)) {
        fprintf(stderr, "The -P (Poisson) and -u (Uniform Range) arguments are mutually exclusive.  You must use one or the other, not both.\n");
        exit(2);
    }

    // Make sure we don't have a uniform distribution range that allows for negative delays
    if ((uniform_range >= 0.0) && ((1.0/lambda - uniform_range/2.0) < 0)) {
        fprintf(stderr, "The range of uniform distribution must not allow negative delay intervals.  Set a higher target interval or a lower uniform range.\n");
        exit(2);
    }

    if ((offset_seconds >= 0) && ((uniform_range >= 0.0) || do_poisson || got_interval_arg || got_rate_arg)) {
        fprintf(stderr, "The -o (offset) argument cannot be used alongside the -u (uniform), -P (Poisson), -i (interval), or -r (rate) arguments.\n");
        exit(2);
    }

    // Make sure we don't have both -j and -q arguments
    if (json && quiet) {
        fprintf(stderr, "The -j (json) and -q (quiet) arguments are mutually exclusive.  You may use one or the other, not both.\n");
        exit(2);
    }

    // Make sure we don't have both -x and -X arguments
    if (got_max_delay && got_max_delay_2) {
        fprintf(stderr, "The -x (max delay limit) and -X (max delay halving) arguments are mutually exclusive.  You must use one or the other, not both.\n");
        exit(2);
    }

    // Make sure the -x bound is >= 0 if set
    if (got_max_delay && max_delay < 0) {
        fprintf(stderr, "The -x (max delay limit) argument must be greater than or equal to zero.\n");
        exit(2);
    }

    // Make sure the =X bound is > 0 if set
    if (got_max_delay_2 && (max_delay_2 <= 0)) {
        fprintf(stderr, "The -X (max delay halving) argument must be greater than zero.\n");
        exit(2);
    }

    // Make sure if we have -x or -X we also have -P (no delay limits without Poisson delay)
    if ((got_max_delay || got_max_delay_2) && !do_poisson) {
        fprintf(stderr, "The -x (max delay limit) and -X (max delay halving) arguments must be used with the -P argument (Poisson delay).\n");
        exit(2);
    }

    // Make sure the destination is a valid IPv4 address
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    if (inet_pton(AF_INET, target_ip, &addr.sin_addr) != 1) {
        fprintf(stderr, "Invalid target IP address: %s\n", target_ip);
        exit(2);
    }
    
    #if DRY_RUN
        // Display arguments and exit, for debugging purposes
        printf("Arguments are:\n\
            Target IP: %s\n\
            Interface: %s\n\
            Count: %d\n\
            Group Size: %u\n\
            Quiet: %u\n\
            JSON: %u\n\
            Lambda: %f\n\
            Size: %u\n\
            Duration: %f\n\
            Timeout: %f\n\
            Max Delay (Limit): %f\n\
            Max Delay (Halving): %f\n\
            Socket Debug: %u\n\
            Set TTL: %u\n\
            Uniform Range: %f\n\
            Do Poisson: %u\n",
            target_ip, bind_ifname, count, group_size, quiet, json, lambda, packet_size, duration, timeout, max_delay, max_delay_2, sock_debug, set_ttl, uniform_range, do_poisson
        );
        exit(0);
    #endif
}

// Listen for echo reply packets
static void* receiver_thread(void* arg) {
    (void)arg;
    char buf[1024];

    while (!atomic_load(&stop_receiver)) {
        // Receive a packet
        struct sockaddr_in from;
        socklen_t fromlen = sizeof(from);
        ssize_t n = recvfrom(sock, buf, sizeof(buf), 0,
                              (struct sockaddr*)&from, &fromlen);
        if (n < 0) {
            if (errno == EINTR || errno == EWOULDBLOCK || errno == EAGAIN) continue;
            perror("recvfrom");
            continue;
        }

        // Compute time since start and current timestamp
        struct timespec now;
        current_time(&now);
        double recv_time = time_diff(start_ts, now);

        // Get the source address from the reply
        char from_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &from.sin_addr, from_str, sizeof(from_str));

        // Make sure the packet is long enough, and get header pointer
        if ((size_t)n < sizeof(struct iphdr) + sizeof(struct icmphdr)) continue;
        struct iphdr* ip_hdr = (struct iphdr* )buf;

        // Make sure the packet is ICMP
        if (!packet_is_icmp(ip_hdr)) continue;

        // Get ICMP header pointer
        int ip_hdr_len = ip_hdr->ihl * 4;
        if ((size_t)n < (size_t)ip_hdr_len + sizeof(struct icmphdr)) continue;
        struct icmphdr* icmph = (struct icmphdr*)(buf + ip_hdr_len);

        // Handle Echo Reply (Type=0, Code=0)
        if (icmp_is_echo_reply(icmph)) {
            // Ignore packets from other PIDs
            if (!same_process(icmph)) continue;

            // Retrieve the sending timestamp for this sequence number
            unsigned short seq = ntohs(icmph->un.echo.sequence);
            double st = get_sent_timestamp(seq);
            if (st < 0.0) continue; // No matching send timestamp found

            // Compute the RTT and update statistics
            double rtt_ms = (recv_time - st) * 1000.0;
            atomic_fetch_add(&recv_count, 1);
            if (rtt_min < 0.0 || rtt_ms < rtt_min) rtt_min = rtt_ms;
            if (rtt_max < 0.0 || rtt_ms > rtt_max) rtt_max = rtt_ms;
            rtt_sum += rtt_ms;

            // Print per-packet output
            print_packet_info(&now, n-ip_hdr_len, from_str, seq, ip_hdr->ttl, rtt_ms, ICMP_TYPE_ECHO_REPLY);
        }
        
        // Handle TTL Exceeded (Type=11, Code=0)
        else if (icmp_is_ttl_exceeded(icmph)) {
            // Get embedded header from original Echo Request
            size_t payload_offset = ip_hdr_len + sizeof(struct icmphdr);
            if ((size_t)n < payload_offset + sizeof(struct iphdr)) continue;
            struct iphdr *inner_ip = (struct iphdr *)(buf + payload_offset);
            if (!packet_is_icmp(inner_ip)) continue;
            size_t inner_ip_len = (size_t)inner_ip->ihl * 4;
            if (inner_ip_len < sizeof(struct iphdr)) continue;
            if ((size_t)n < payload_offset + inner_ip_len + sizeof(struct icmphdr)) continue;
            struct icmphdr *inner_icmp = (struct icmphdr *)(buf + payload_offset + inner_ip_len);
            if (!icmp_is_echo_request(inner_icmp)) continue;

            // Make sure the expired Echo Request was one we sent
            if (!same_process(inner_icmp)) continue;

            // Get the sequence number for the expired Echo Request and retrieve its sending timestamp
            unsigned short seq = ntohs(inner_icmp->un.echo.sequence);
            double st = get_sent_timestamp(seq);
            if (st < 0.0) continue; // No matching send timestamp found

            // Compute the RTT from where TTL expired
            double rtt_ms = (recv_time - st) * 1000.0;

            // Print per-packet output
            print_packet_info(&now, n-ip_hdr_len, from_str, seq, ip_hdr->ttl, rtt_ms, ICMP_TYPE_TTL_EXCEEDED);
        }
    }
    return NULL;
}

int main(int argc, char* argv[]) {
    // Initialize random number generation
    srand(time(NULL));

    // Get process ID
    pid = getpid();

    // Prepare to handle interrupts
    start_interrupt_handler();

    // Parse command-line arguments (results will be stored in static variables)
    parse_args(argc, argv);

    // Initialize array to store timestamps
    for (int i=0; i<SEQ_TABLE_SIZE; i++) {
        sent_time[i] = -1.0;
    }

    // Create a socket
    sock = socket(AF_INET, SOCK_RAW, IPPROTO_ICMP);
    if (sock < 0) {
        fprintf(stderr, "Failed to create socket, do you have root privileges?\n");
		exit(2);
    }

    // Set socket debug option
    set_socket_debug(sock, sock_debug);

    // Set socket timeout option
    set_socket_timeout(sock, timeout);

    if (bind_ifname != NULL) {
        if (set_socket_bind_ifname(sock, bind_ifname) < 0) {
            fprintf(stderr, "Failed to bind to device with name '%s'.  Make sure the interface exists and you have root privileges.\n", bind_ifname);
            close(sock);
            exit(2);
        }
    }

    if (set_ttl > 0) {
        if (set_socket_ttl(sock, set_ttl) < 0) {
            fprintf(stderr, "Failed to set socket TTL=%u.\n", set_ttl);
            close(sock);
            exit(2);
        }
    }

    // Start receiver thread to listen for Echo Reply packets
    pthread_t recv_tid;
    if (pthread_create(&recv_tid, NULL, receiver_thread, NULL) != 0) {
        perror("pthread_create");
        close(sock);
        exit(2);
    }

    // Create ICMP packet
    char packet[packet_size];
    memset(packet, 0, packet_size);
    struct icmphdr* icmph = (struct icmphdr*)packet;
    icmph->type = 8;
    icmph->code = 0;
    icmph->un.echo.id = htons((unsigned short)(pid & 0xFFFF));
    for (size_t i = sizeof(struct icmphdr); i < packet_size; i++) {
        packet[i] = (char)(i & 0xFF);
    }
    
    if (json) printf("[\n");
    else printf("PPING %s (%s) %u(%u) bytes of data.\n", target_ip, target_ip, packet_size-8, packet_size+20);

    current_time(&start_ts);
    if (offset_seconds >= 0.0) {
        struct timespec offset_ts;
        offset_delay(&offset_ts, offset_seconds);
        nanosleep(&offset_ts, NULL);
    }

    double elapsed = 0.0;
    int seq = 1;
    // Start sending packets.  Continue until count/duration is exceeded or Ctrl-C is pressed
    while ((elapsed < duration || duration < 0) && (seq <= count || count < 0) && !atomic_load(&stop_sender)) {
        for (int i=0; i<group_size; i++) {
            // Update sequence number and recompute checksum
            icmph->un.echo.sequence = htons((unsigned short)seq);
            icmph->checksum = 0;
            icmph->checksum = checksum((unsigned short*)packet, sizeof(packet));

            // Compute timestamp relative to start time and store it for later
            double send_ts = now_elapsed();
            set_sent_timestamp(seq, send_ts);

            // Send packet
            ssize_t sent = sendto(sock, packet, sizeof(packet), 0,
                                    (struct sockaddr*)&addr, sizeof(addr));
            if (sent < 0) perror("sendto");
            else atomic_fetch_add(&sent_count, 1);
            
            // Wait for some amount of time determined by Poisson distribution
            seq += 1;
            if (seq > count && count >= 0) {
                break;
            }
        }
        if ((seq <= count || count < 0) && !atomic_load(&stop_sender)) {
            struct timespec delay_ts;
            if (offset_seconds >= 0.0) {
                offset_delay(&delay_ts, offset_seconds);
                nanosleep(&delay_ts, NULL);
            } else if (lambda > 0) {
                if (do_poisson) {
                    poisson_delay(&delay_ts, lambda);
                } else if (uniform_range >= 0.0) {
                    uniform_delay(&delay_ts, lambda, uniform_range);
                } else {
                    fixed_delay(&delay_ts, lambda);
                }
                nanosleep(&delay_ts, NULL);
            } else {
                zero_delay(&delay_ts);
            }
            elapsed = now_elapsed();

            if ((elapsed < duration || duration < 0) && (lambda > 0)){
                // Update interval statistics
                double int_ms = timespec_to_msec(&delay_ts);
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
    int n_sent = atomic_load(&sent_count);
    int n_recv = atomic_load(&recv_count);

    if (json) {
        printf("\n]\n");
    } else {    
        double loss_pct = 0.0;
        if (n_sent > 0) {
            loss_pct = ((n_sent - n_recv) / n_sent) * 100.0;
        }
        printf("\n--- %s pping statistics ---\n", target_ip);
        double total_duration = now_elapsed()-timeout;
        printf("%d packets transmitted, %d received, %.3f%% packet loss, time %.3fms\n", n_sent, n_recv, loss_pct, (total_duration*1000.0));
        
        // RTT statistics require at least one packet received
        if (n_recv > 0) {
            printf("rtt min/avg/max = %.3f/%.3f/%.3f ms\n",
                rtt_min, rtt_sum / n_recv, rtt_max);
        }

        // Statistics about inter-packet intervals require at least two packets sent
        if (n_sent > 1) {
            if (lambda <= 0) {
                int_min = 0; int_max = 0;
            }
            printf("interval min/avg/max = %.3f/%.3f/%.3f ms\n",
                int_min, int_sum / (floor(n_sent/group_size)-1), int_max); // Floor in case last group is smaller.  Minus 1 because we're measuring gaps

            double pps = (double)n_sent/total_duration;
            printf("pps avg = %.3f\n", pps);
        } else {
            printf("interval min/avg/max = -1/-1/-1 ms\n");
            printf("pps avg = -1\n"); 
        }
    }

    // Close the socket
    close(sock);

    // Match regular ping's exit status
    // If no responses received, return 1
    // If deadline and count are both specified and responses received is less than count, return 1
    // Otherwise, return 0
    if (n_recv == 0) return 1;
    if (count > 0 && duration > 0 && n_recv < count) return 1;
    return 0;
}
