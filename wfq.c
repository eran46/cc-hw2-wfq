#define _CRT_SECURE_NO_WARNINGS  // disable warnings
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>    // for LLONG_MAX

#define INITIAL_CAPACITY 1024
#define MAX_LINE_LEN     1024
#define MAX_KEY_LEN      256

typedef struct { // flow data struct for initial pass
    char* key;
    double last_finish;
    int    weight;
    int    priority;
} flow_info;

typedef struct { // packet data struct for WFQ
    long long arrival;
    long long length;
    double    finish_tag;
    int       priority;
    char* record;
} Packet;


int main(void) {
    // fprintf(stderr, "DEBUG: WFQ scheduler starting up\n");

    /* we use an array of flow_info structs to keep track of flows, priorities etc.*/
    flow_info* flows = malloc(sizeof(*flows) * INITIAL_CAPACITY); 
    /* we use an array of Packet structs to keep track of each packet's WFQ parameters */
    Packet* pkts = malloc(sizeof(*pkts) * INITIAL_CAPACITY);

    if (!flows || !pkts) {
        fprintf(stderr, "ERROR: malloc failed\n");
        return 1;
    }

    int flows_cap = INITIAL_CAPACITY, flows_cnt = 0;
    int pkts_cap = INITIAL_CAPACITY, pkts_cnt = 0;
    char line[MAX_LINE_LEN], keybuf[MAX_KEY_LEN];
    int next_prio = 0;

    /* --- read and parse input lines from stdin --- */
    while (fgets(line, sizeof(line), stdin)) { // O(M)
        long long t, L;
        char src_ip[64], src_prt[16], dst_ip[64], dst_prt[16];
        int matched, w = 0;
        bool has_w;
        char* nl;

        if ((nl = strchr(line, '\n'))) *nl = '\0';

        matched = sscanf_s(
            line,
            "%lld %63s %15s %63s %15s %lld %d",
            &t,
            src_ip, (unsigned)sizeof(src_ip),
            src_prt, (unsigned)sizeof(src_prt),
            dst_ip, (unsigned)sizeof(dst_ip),
            dst_prt, (unsigned)sizeof(dst_prt),
            &L, &w
        );
        if (matched < 6) continue;
        has_w = (matched == 7); // check for weight argument in line

        /* key defining the flow with which to search wether a flow exists already*/
        snprintf(keybuf, sizeof(keybuf), "%s:%s-%s:%s",
            src_ip, src_prt, dst_ip, dst_prt);

        /* find or create flow entry */
        int idx = -1;
        for (int i = 0; i < flows_cnt; i++) { // iterate on all current flows - O(M)
            if (strcmp(flows[i].key, keybuf) == 0) {
                idx = i;
                break;
            }
        }
        if (idx < 0) {
            if (flows_cnt == flows_cap) { // house keeping
                flows_cap *= 2;
                flows = realloc(flows, sizeof(*flows) * flows_cap);
                if (!flows) { perror("realloc"); return 1; }
            }
            idx = flows_cnt++;
            flows[idx].key = _strdup(keybuf);
            flows[idx].last_finish = 0.0;
            flows[idx].weight = 1;
            flows[idx].priority = next_prio++;
        }

        if (has_w) {
            flows[idx].weight = w;
        }

        /* compute finish?tag */
        double S = flows[idx].last_finish > (double)t
            ? flows[idx].last_finish
            : (double)t;
        // adjusted WFQ formula without tracking global virtual time
        double F = S + (double)L / (double)flows[idx].weight;
        flows[idx].last_finish = F;

        /* update packet info in pkts */
        if (pkts_cnt == pkts_cap) {
            pkts_cap *= 2;
            pkts = realloc(pkts, sizeof(*pkts) * pkts_cap);
            if (!pkts) { perror("realloc"); return 1; }
        }
        pkts[pkts_cnt].arrival = t;
        pkts[pkts_cnt].length = L;
        pkts[pkts_cnt].finish_tag = F;
        pkts[pkts_cnt].priority = flows[idx].priority;
        pkts[pkts_cnt].record = _strdup(line);
        pkts_cnt++;
    }

    /*fprintf(stderr,
        "DEBUG: Finished reading input ? flows=%d, packets=%d\n",
        flows_cnt, pkts_cnt
    );*/


    /* --- WFQ --- */

    //fprintf(stderr, "DEBUG: Starting WFQ simulation\n");
    bool* sent = calloc(pkts_cnt, sizeof(bool));
    int       sent_cnt = 0;
    long long cur_time = 0;

    /* debug */
    /*fprintf(stderr, "---- LOOP ITERATION, cur_time=%lld sent_cnt=%d/%d ----\n",
        cur_time, sent_cnt, pkts_cnt);
    for (int i = 0; i < pkts_cnt; i++) {
        if (!sent[i]) {
            fprintf(stderr, " pkt[%2d]: arrival=%lld  F=%.3f  prio=%d\n",
                i, pkts[i].arrival,
                pkts[i].finish_tag,
                pkts[i].priority);
        }
    }*/

    while (sent_cnt < pkts_cnt) { // sum 1 to pkts_cnt - O(M^2)

        long long next_arrival = LLONG_MAX;
        int       best = -1;

        for (int i = 0; i < pkts_cnt; i++) { // O(M)
            if (sent[i]) continue;
            if (pkts[i].arrival <= cur_time) { // simulate on-line behavior
                if (best < 0
                    || pkts[i].finish_tag < pkts[best].finish_tag
                    || (pkts[i].finish_tag == pkts[best].finish_tag
                        && pkts[i].priority < pkts[best].priority)) // min finish_tag, priority as tie breaker
                {
                    best = i;
                }
            }
            else {
                if (pkts[i].arrival < next_arrival)
                    next_arrival = pkts[i].arrival;
            }
        }

        /* if none ready, fast-forward time to the next arrival */
        if (best < 0) {
            cur_time = next_arrival;
            continue;
        }

        /* send chosen packet */
        Packet* p = &pkts[best];
        long long start = cur_time > p->arrival
            ? cur_time
            : p->arrival;
        printf("%lld: %s\n", start, p->record);
        /*fprintf(stderr,
            "DEBUG: Emitted Packet[%d]: start=%lld, rec=\"%s\"\n",
            best, start, p->record
        );*/

        cur_time = start + p->length;
        sent[best] = true;
        sent_cnt++;
    }

    free(sent);

    /* --- cleaning mem --- */
    for (int i = 0; i < flows_cnt; i++)  free(flows[i].key);
    for (int i = 0; i < pkts_cnt; i++)  free(pkts[i].record);
    free(flows);
    free(pkts);

    // fprintf(stderr, "DEBUG: WFQ scheduler done\n");
    return 0;
}
