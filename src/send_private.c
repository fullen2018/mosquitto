/*
Copyright (c) 2009-2016 Roger Light <roger@atchoo.org>

All rights reserved. This program and the accompanying materials
are made available under the terms of the Eclipse Public License v1.0
and Eclipse Distribution License v1.0 which accompany this distribution.

The Eclipse Public License is available at
   http://www.eclipse.org/legal/epl-v10.html
and the Eclipse Distribution License is available at
  http://www.eclipse.org/org/documents/edl-v10.php.

Contributors:
   Zhan Jianhui - Simple implementation cluster.
*/

#include "config.h"
#include <assert.h>

#include "mosquitto_broker.h"
#include "mqtt3_protocol.h"
#include "memory_mosq.h"
#include "util_mosq.h"

#ifdef WITH_CLUSTER
int send__private_subscribe(struct mosquitto *context, int *mid, const char *topic, uint8_t topic_qos, char *client_id, uint16_t sub_id)
{
	/* FIXME - only deals with a single topic */
	struct mosquitto__packet *packet = NULL;
	uint32_t packetlen;
	uint16_t local_mid;
	int rc;

	assert(context);
	assert(topic);

	if(context->sock == INVALID_SOCKET) return MOSQ_ERR_NO_CONN;

	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	packetlen = 2/* 0. Message Identifier */ + 2/* 1. topic length */ +strlen(topic) + 1/* 2. QoS */ + 
                2/* 3. client id length*/ + strlen(client_id) + 2/* 4. sub_id */;

	packet->command = PRIVATE | PRIVATE_SUBSCRIBE;
	packet->remaining_length = packetlen;
	rc = packet__alloc(packet);
	if(rc){
		mosquitto__free(packet);
		return rc;
	}

	/* Variable header */
	local_mid = mosquitto__mid_generate(context);
	if(mid) *mid = (int)local_mid;
    /* 0. mid */
	packet__write_uint16(packet, local_mid);
	/* 1. topic */
	packet__write_string(packet, topic, strlen(topic));
    /* 2. QoS */
	packet__write_byte(packet, topic_qos);
    /* 3. original client id */
	packet__write_string(packet, client_id, strlen(client_id));
	/* 4. sub id */
	packet__write_uint16(packet, sub_id);

	//zhan jianhui debug
    log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Sending private subscribe to node:%s (sub_client_id:%s subid:%d topic:%s qos:%d mid:%d)",
								      context->id, client_id, sub_id, topic, topic_qos, local_mid);

	return packet__queue(context, packet);
}

int send__private_retain(struct mosquitto *context, char *remote_client_id, uint16_t sub_id, const char* topic, uint8_t qos, int mid, int64_t rcv_time, uint32_t payloadlen, const void *payload)
{
	struct mosquitto__packet *packet = NULL;
	int packetlen;
	int rc;
	int64_t remote_rcv_time;

	assert(context && context->is_peer);
	assert(topic);
	if(context->sock == INVALID_SOCKET) return MOSQ_ERR_NO_CONN;

	packetlen = 2/* 1. topic length */ + strlen(topic) + 1/* 2. QoS*/ + 
		        2/* 4. remote client id length */ + strlen(remote_client_id) + 2/* 5. sub id */ +
		        /* 6. original rcv time */ + 8 + payloadlen;
	if(qos > 0) packetlen += 2; /* 3. message id */
	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	packet->mid = mid;
	packet->command = PRIVATE | PRIVATE_RETAIN;
	packet->remaining_length = packetlen;
	rc = packet__alloc(packet);
	if(rc){
		mosquitto__free(packet);
		return rc;
	}
	/* 1. topic*/
	packet__write_string(packet, topic, strlen(topic));

	/* 2. QoS */
	packet__write_byte(packet, qos);

	/* 3. mid */
	if(qos > 0){
		packet__write_uint16(packet, mid);
	}

	/* 4. remote client id */
	if(remote_client_id){
		packet__write_string(packet, remote_client_id, strlen(remote_client_id));
	}

	/* 5. sub id */
	packet__write_uint16(packet, sub_id);

	/* 6. Retain Rcv Time */
	remote_rcv_time = rcv_time + context->remote_time_offset;
	packet__write_int64(packet, remote_rcv_time);

	/* 7. payload */
	if(payloadlen){
		packet__write_bytes(packet, payload, payloadlen);
	}

	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Sending private retain to peer:%s (remote_client_id:%s subid:%d topic:%s qos:%d mid:%d remote_rcv_time:%ld payloadlen:%d, local_rcv_time:%ld, time_off_set:%ld)",
		                              context->id, remote_client_id, sub_id, topic, qos, mid, remote_rcv_time, payloadlen, rcv_time, context->remote_time_offset);

	return packet__queue(context, packet);
}

int send__session_req(struct mosquitto *node, char *client_id, uint8_t clean_session)
{
	struct mosquitto__packet *packet = NULL;
	uint32_t packetlen;
	int rc;

	assert(node);
	assert(client_id);
	if(node->sock == INVALID_SOCKET) return MOSQ_ERR_NO_CONN;

	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	packetlen = 2 + strlen(client_id) + 1;

	packet->command = PRIVATE | SESSION_REQ;
	packet->remaining_length = packetlen;
	rc = packet__alloc(packet);
	if(rc){
		mosquitto__free(packet);
		return rc;
	}

	/* client id */
	packet__write_string(packet, client_id, strlen(client_id));

	/* clean session */
	packet__write_byte(packet, clean_session);

	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Sending session request to node: %s, client_id: %s(%ld bytes)",
										  node->id, client_id, strlen(client_id));

	return packet__queue(node, packet);
}


int send__session_resp(struct mosquitto *peer, char *client_id, struct mosquitto *client_context)
{
	struct mosquitto__packet *packet = NULL;
	struct client_topic_table *client_sub = NULL;
	struct mosquitto_client_msg *clientmsg = NULL;
	struct mosquitto_msg_store *pubmsg = NULL;
	uint8_t sub_qos = 0, flag;
	uint16_t nrsubs = 0, nrpubs = 0;
	uint32_t packetlen = 0;
	int rc, i, payload_len = 0;

	assert(peer);
	assert(client_id);
	if(peer->sock == INVALID_SOCKET) return MOSQ_ERR_NO_CONN;

	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	/* client id + last_mid + num of subs + sub1(topic/qos) + sub2 + ... + num of pubs + pub1(topic/qos/mid/payload) + pub2(topic/qos/mid/payload) + ... */
	packetlen = 2 + strlen(client_id) + 2 + 2 + 2;

	packet->command = PRIVATE | SESSION_RESP;

	/* caculate total subs length and num of subs */
	for(i = 0; i < client_context->client_topic_count; i++){
		client_sub = client_context->client_topics[i];
		if(client_sub){   /* topic_len + topic + qos */
			payload_len += (2 + strlen(client_sub->topic_tbl->topic_payload)+ 1);
			nrsubs++;
		}
	}

	/* caculate pubs length and num of pubs */
	clientmsg = client_context->msgs;
	while(clientmsg){
		if(clientmsg->qos > 0 && clientmsg->state != mosq_ms_invalid && clientmsg->state != mosq_ms_publish_qos0 && !clientmsg->retain){
			pubmsg = clientmsg->store;
			/* topic_len + topic + flag(state|dir|dup|qos) + mid + payloadlen + payload */
			payload_len += 2 + strlen(pubmsg->topic) + 1 + 2 + 4 + pubmsg->payloadlen;
			nrpubs++;
		}
		clientmsg = clientmsg->next;
	}

	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] sending SESSION RESP to peer: %s for client: %s, mid:%d, total %d SUBs, %d PUBs",
										  peer->id, client_context->id, client_context->last_mid, nrsubs, nrpubs);
	packet->remaining_length = packetlen + payload_len;

	rc = packet__alloc(packet);
	if(rc){
		mosquitto__free(packet);
		return rc;
	}

	/* client id */
	packet__write_string(packet, client_id, strlen(client_id));

	/* last mid */
	packet__write_uint16(packet, client_context->last_mid);

	/* num of subs */
	packet__write_uint16(packet, nrsubs);

	/* sub1, sub2, ... */
	for(i = 0; i < client_context->client_topic_count; i++){
		client_sub = client_context->client_topics[i];
		if(client_sub){
			packet__write_string(packet, client_sub->topic_tbl->topic_payload, strlen(client_sub->topic_tbl->topic_payload));
			packet__write_byte(packet, client_sub->sub_qos);

			char *tmp_sub_topic = mosquitto__malloc(strlen(client_sub->topic_tbl->topic_payload) + 1);
			memcpy(tmp_sub_topic, client_sub->topic_tbl->topic_payload, strlen(client_sub->topic_tbl->topic_payload));
			tmp_sub_topic[strlen(client_sub->topic_tbl->topic_payload)]='\0';
			log__printf(NULL, MOSQ_LOG_DEBUG, "\tSESSION RESP(SUBs): topic:%s(%ld bytes) qos:%d(out)",
										  tmp_sub_topic,strlen(tmp_sub_topic), sub_qos);
			mosquitto__free(tmp_sub_topic);
		}
	}

	/* num of pubs */
	packet__write_uint16(packet, nrpubs);

	/* pub1, pub2, ... */
	clientmsg = client_context->msgs;
	while(clientmsg){
		if(clientmsg->qos > 0 && clientmsg->state != mosq_ms_invalid && clientmsg->state != mosq_ms_publish_qos0 && !clientmsg->retain){
			pubmsg = clientmsg->store;
			packet__write_string(packet, pubmsg->topic, strlen(pubmsg->topic));
			/* flag = (state|dir|dup|qos) */
			flag = (((uint8_t)clientmsg->state)&0x0F << 4) + (((uint8_t)clientmsg->direction)&0x01 << 3)
					+ (((uint8_t)clientmsg->dup)&0x01 << 2) + (((uint8_t)clientmsg->state)&0x03);
			packet__write_byte(packet, flag);
			packet__write_uint16(packet, clientmsg->mid);
			packet__write_uint32(packet, pubmsg->payloadlen);
			packet__write_string(packet, UHPA_ACCESS_PAYLOAD(pubmsg), pubmsg->payloadlen);

			char *tmp_payload = mosquitto__malloc(pubmsg->payloadlen + 1);
			memcpy(tmp_payload, pubmsg->payload, pubmsg->payloadlen);
			tmp_payload[pubmsg->payloadlen] = '\0';
			log__printf(NULL, MOSQ_LOG_DEBUG, "\tSESSION RESP(PUBs): topic:%s flag(state|dir|dup|qos):0x%02X mid:%d payload:%s(out)",
										  pubmsg->topic, flag, clientmsg->mid, tmp_payload);
			mosquitto__free(tmp_payload);

			db__msg_store_deref(client_context->db, &pubmsg);
		}

		clientmsg = clientmsg->next;
	}

	clientmsg = client_context->queued_msgs;
	while(clientmsg){
		if(clientmsg->qos > 0 && clientmsg->state != mosq_ms_invalid && clientmsg->state != mosq_ms_publish_qos0 && !clientmsg->retain){
			pubmsg = clientmsg->store;
			packet__write_string(packet, pubmsg->topic, strlen(pubmsg->topic));
			flag = (((uint8_t)clientmsg->state)&0x0F << 4) + (((uint8_t)clientmsg->direction)&0x01 << 3)
				+ (((uint8_t)clientmsg->dup)&0x01 << 2) + (((uint8_t)clientmsg->state)&0x03);
			packet__write_byte(packet, flag);
			packet__write_uint16(packet, clientmsg->mid);
			packet__write_uint32(packet, pubmsg->payloadlen);
			packet__write_string(packet, UHPA_ACCESS_PAYLOAD(pubmsg), pubmsg->payloadlen);

			char *tmp_payload = mosquitto__malloc(pubmsg->payloadlen + 1);
			memcpy(tmp_payload, pubmsg->payload.ptr, pubmsg->payloadlen);
			tmp_payload[pubmsg->payloadlen] = '\0';
			log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] SESSION RESP(PUBS): topic:%s flag(state|dir|dup|qos):0x%02X mid:%d payload:%s(out)",
												pubmsg->topic, flag, clientmsg->mid, tmp_payload);
			mosquitto__free(tmp_payload);

			db__msg_store_deref(client_context->db, &pubmsg);
		}
		clientmsg = clientmsg->next;
	}

	return packet__queue(peer, packet);
}

int send__multi_subscribes(struct mosquitto *context, int *mid, char **topic_arr, int topic_arr_len)
{
	/* stub sub qos with 0 inside cluster */
	struct mosquitto__packet *packet = NULL;
	uint32_t packetlen;
	uint16_t local_mid;
	int rc, i;

	assert(context);
	assert(topic_arr);

	if(!topic_arr_len) return MOSQ_ERR_SUCCESS;

	packetlen = 2; /* mid */

	for(i=0; i<topic_arr_len; i++){/* topic_len + topic + qos */
		packetlen += (2+strlen(topic_arr[i])+1);
	}
    
	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet));
	if(!packet) return MOSQ_ERR_NOMEM;
    /* mid + (topic_len + topic + qos) * nrtopics */

	packet->command = SUBSCRIBE | (1<<1);
	packet->remaining_length = packetlen;
	rc = packet__alloc(packet);
	if(rc){
		mosquitto__free(packet);
		return rc;
	}

	/* Variable header */
	local_mid = mosquitto__mid_generate(context);
	if(mid) *mid = (int)local_mid;
	packet__write_uint16(packet, local_mid);

	/* Payload */
	for(i=0; i<topic_arr_len; i++){
		packet__write_string(packet, topic_arr[i], strlen(topic_arr[i]));
		packet__write_byte(packet, 0); /* stub qos to 0 */
	}

	log__printf(context, MOSQ_LOG_DEBUG, "[CLUSTER] sending SUBSCRIBE to node: %s (nrTopics:%d,Mid: %d)", context->id, topic_arr_len, local_mid);
	for(i=0; i<topic_arr_len; i++){
		log__printf(context, MOSQ_LOG_DEBUG, "\tSUBSCRIBE topic[%d]: %s", i, topic_arr[i]);
	}

	return packet__queue(context, packet);
}

int send__multi_unsubscribe(struct mosquitto *context, int *mid, char **topic_arr, int topic_arr_len)
{
	struct mosquitto__packet *packet = NULL;
	uint32_t packetlen;
	uint16_t local_mid;
	int rc, i;

	assert(context);
	assert(topic_arr);
	if(!topic_arr_len) return MOSQ_ERR_SUCCESS;

	packet = mosquitto__calloc(1, sizeof(struct mosquitto__packet));
	if(!packet) return MOSQ_ERR_NOMEM;

	packetlen = 2;

	for(i=0; i<topic_arr_len; i++){/* topic_len + topic + qos */
		packetlen += (2+strlen(topic_arr[i]));
	}

	packet->command = UNSUBSCRIBE | (1<<1);
	packet->remaining_length = packetlen;
	rc = packet__alloc(packet);
	if(rc){
		mosquitto__free(packet);
		return rc;
	}

	/* Variable header */
	local_mid = mosquitto__mid_generate(context);
	if(mid) *mid = (int)local_mid;
	packet__write_uint16(packet, local_mid);

	/* Payload */
	for(i=0; i<topic_arr_len; i++)
		packet__write_string(packet, topic_arr[i], strlen(topic_arr[i]));

	log__printf(context, MOSQ_LOG_DEBUG, "[CLUSTER] sending MULTI UNSUBSCRIBE to node: %s (nrTopics:%d,Mid: %d)", context->id, topic_arr_len, local_mid);
	for(i=0; i<topic_arr_len; i++){
		log__printf(context, MOSQ_LOG_DEBUG, "\tMULTI UNSUBSCRIBE topic[%d]: %s", i, topic_arr[i]);
	}

	return packet__queue(context, packet);
}

#endif
