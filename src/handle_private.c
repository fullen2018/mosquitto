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
   Roger Light - initial implementation and documentation.
*/

#include <stdio.h>
#include <string.h>
#include <assert.h>

#include "config.h"

#include "mosquitto_broker.h"
#include "mosquitto_internal.h"
#include "memory_mosq.h"
#include "send_mosq.h"

#ifdef WITH_CLUSTER
int handle__private(struct mosquitto_db *db, struct mosquitto *context)
{
	assert(context);
	switch((context->in_packet.command)&0x0F){
		case PRIVATE_SUBSCRIBE:
			return handle__private_subscribe(db, context);
		case PRIVATE_RETAIN:
			return handle__private_retain(db, context);
		case SESSION_REQ:
			return handle__session_req(db, context);
		case SESSION_RESP:
			return handle__session_resp(db, context);
		default:
			return MOSQ_ERR_PROTOCOL;
	}
}

int handle__private_subscribe(struct mosquitto_db *db, struct mosquitto *context)
{
	int rc = 0;
	int rc2 = 0;
	uint16_t mid = 0;
	char *sub = NULL;
	uint8_t qos = 0;
	char *client_id = NULL;

	/* must from peer */
	if(!(context && context->is_peer)) return MOSQ_ERR_PROTOCOL;
	/* 0. mid */
	if(packet__read_uint16(&context->in_packet, &mid)) return 1;
	/*currently should be only one topic*/
	if(context->in_packet.pos < context->in_packet.remaining_length) {
		sub = NULL;
		if(packet__read_string(&context->in_packet, &sub)){/* 1. topic */
			return 1;
		}
		if(sub){
			if(packet__read_byte(&context->in_packet, &qos)){/* 2. QoS */
				mosquitto__free(sub);
				return 1;
			}

			if(packet__read_string(&context->in_packet, &client_id)){/* 3. original client id */
				mosquitto__free(sub);
				return 1;
			}

			if(context->last_sub_client_id)
				mosquitto__free(context->last_sub_client_id);
			context->last_sub_client_id = client_id;

			if(packet__read_uint16(&context->in_packet, &context->last_sub_id)){/* 4. sub id */
				mosquitto__free(sub);
				return 1;
			}

			log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Received PRIVATE SUBSCRIBE from peer: %s, topic: %s, client_id: %s, sub_id: %d", 
												context->id,
												sub,
												client_id,
												context->last_sub_id);
			rc2 = sub__add(db, context, sub, qos, &db->subs);
			if(rc2 == MOSQ_ERR_SUCCESS){
				if(sub__retain_queue(db, context, sub, qos)) rc = 1;
			}else if(rc2 != -1){
				rc = rc2;
			}
			mosquitto__free(sub);
		}
	}
	return rc;
}

int handle__private_retain(struct mosquitto_db *db, struct mosquitto *context)
{
//read client id, orig rcv time
//find client context
//compare and swap retain msg by orig rcv time
//drop or insert to client retain queue
	char *topic = NULL, *client_id = NULL;
	uint8_t qos = 0;
	uint16_t mid = 0, sub_id = 0;
	uint32_t payloadlen = 0;
	int64_t orig_rcv_time = 0;

	mosquitto__payload_uhpa payload;
	struct mosquitto_msg_store *stored = NULL;
	struct mosquitto_client_msg *cr_msg = NULL, *tmp_cr_msg = NULL, *prev_cr_msg = NULL;
	struct mosquitto_client_retain *cr = NULL;
	struct mosquitto *client = NULL;

	/* must from remote node */
	if(!(context && context->is_node)) return MOSQ_ERR_PROTOCOL;

	if(packet__read_string(&context->in_packet, &topic)){/* 1. topic */
		return 1;
	}

	if(packet__read_byte(&context->in_packet, &qos)){/* 2. QoS */
		mosquitto__free(topic);
		return 1;
	}

	if(qos >0){
		if(packet__read_uint16(&context->in_packet, &mid)){/* 3. mid */
			mosquitto__free(topic);
			return 1;
		}
	}

	if(packet__read_string(&context->in_packet, &client_id)){/* 4. client_id */
		mosquitto__free(topic);
		return 1;
	}

	HASH_FIND(hh_id, db->contexts_by_id, client_id, strlen(client_id), client);
	if(!client){
		mosquitto__free(topic);
		return 1;
	}

	if(packet__read_uint16(&context->in_packet, &sub_id)){/* 5. sub id */
		mosquitto__free(topic);
		return 1;
	}

	if(packet__read_int64(&context->in_packet, &orig_rcv_time)){/* 6. retain msg recv time */
		mosquitto__free(topic);
		mosquitto__free(client_id);
		return 1;
	}

	cr = db->retain_list;
	while(cr){
		if(cr->sub_id == sub_id)
			break;
		cr = cr->next;
	}

	if(cr) cr_msg = cr->retain_msgs;

	tmp_cr_msg = NULL;
	time_t now = mosquitto_time();
	while(cr_msg){/* find all same topic retain msg, remove the stale ones. */
		if(!strcmp(cr_msg->store->topic, topic)){
			if((int64_t)cr_msg->timestamp >= orig_rcv_time){/* discard this msg */
				log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Receive private retain from node:%s at:%ld (orig_client_id:%s subid:%d topic:%s qos:%d mid:%d orig_rcv_time:%ld payloadlen:%d), but local has a fresh retain(%ld)",
															context->id, (int64_t)now, client_id, sub_id, topic, qos, mid, orig_rcv_time, context->in_packet.remaining_length - context->in_packet.pos, (int64_t)cr_msg->timestamp);
				mosquitto__free(topic);
				mosquitto__free(client_id);

				return MOSQ_ERR_SUCCESS; /* continue?? */
			}else{
				if(cr_msg == cr->retain_msgs){
					cr->retain_msgs = cr->retain_msgs->next;
				}else if(cr_msg == cr->retain_msgs->next){
					prev_cr_msg = cr->retain_msgs;
				}
				tmp_cr_msg = cr_msg;
				cr_msg = cr_msg->next;
				if(prev_cr_msg)
					prev_cr_msg->next = cr_msg;
				mosquitto__free(tmp_cr_msg);
				db__msg_store_deref(db, &tmp_cr_msg->store);
			}
		}else{
			if(cr_msg == cr->retain_msgs->next){
				prev_cr_msg = cr->retain_msgs;
			}
			cr_msg = cr_msg->next;
			if(prev_cr_msg)
				prev_cr_msg = prev_cr_msg->next;
		}
	}

	payloadlen = context->in_packet.remaining_length - context->in_packet.pos;

	if(payloadlen){
		if(db->config->message_size_limit && payloadlen > db->config->message_size_limit){
			log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Dropped too large PUBLISH from peer: %s (q%d, r%d, m%d, '%s', ... (%ld bytes))", context->id, qos, true, mid, topic, (long)payloadlen);
			mosquitto__free(topic);
			mosquitto__free(client_id);
			return 1;
		}
		if(UHPA_ALLOC(payload, payloadlen+1) == 0){
			mosquitto__free(topic);
			mosquitto__free(client_id);
			return 1;
		}
		if(packet__read_bytes(&context->in_packet, UHPA_ACCESS(payload, payloadlen), payloadlen)){/* 7. payload */
			mosquitto__free(topic);
			mosquitto__free(client_id);
			UHPA_FREE(payload, payloadlen);
			return 1;
		}
	}else{
		mosquitto__free(topic);
		mosquitto__free(client_id);
		return 1;
	}

	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Receive private retain from node:%s at:%ld (orig_client_id:%s subid:%d topic:%s qos:%d mid:%d orig_rcv_time:%ld payloadlen:%d)",
												context->id, (int64_t)now, client_id, sub_id, topic, qos, mid, orig_rcv_time, payloadlen);

	db__message_store(db, context->id, mid, topic, qos, payloadlen, payload, true, &stored, 0);
	stored->rcv_time = (time_t)orig_rcv_time;/* this is the absolute local time when other node recv this retain msg. */
	stored->from_node = true;    
	sub__messages_queue(db, NULL, topic, qos, true, &stored);
	return db__message_insert_into_retain_queue(db, client, mid, mosq_md_out, qos, true, stored, sub_id);
}

int handle__session_req(struct mosquitto_db *db, struct mosquitto *context)
{
	int rc = 0;
	char *client_id = NULL;
	uint8_t clean_session;
	struct mosquitto *client_context = NULL;

	if(!context->is_peer)
		return 1;

	if(packet__read_string(&context->in_packet, &client_id))
		return 1;

	if(packet__read_byte(&context->in_packet, &clean_session))
		return 1;

	HASH_FIND(hh_id,db->contexts_by_id, client_id, strlen(client_id), client_context);
	if(!client_context){/* just ignore this msg if client id not founded. */
		log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Receive SESSION REQ from peer: %s, client_id:%s(%ld bytes) not found in local db.", context->id, client_id,strlen(client_id));
		mosquitto__free(client_id);

		return MOSQ_ERR_SUCCESS;
	}
	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Receive SESSION REQ from peer: %s, client_id:%s(%ld bytes) has found in local db", context->id, client_id,strlen(client_id));
	if(!clean_session)
		rc = _mosquitto_send_session_resp(context, client_id, client_context);

	log__printf(NULL, MOSQ_LOG_ERR, "[CLUSTER] Client %s(%ld bytes) has been connected to remote peer, closing old connection.", client_id,strlen(client_id));
	mosquitto__free(client_id);
	client_context->clean_session = true;
	//client_context->state = mosq_cs_disconnected;
	client_context->save_subs = false;
	do_disconnect(db, client_context);

	return rc;
}

int handle__session_resp(struct mosquitto_db *db, struct mosquitto *context)
{
	char *client_id = NULL, *sub_topic = NULL, *pub_topic = NULL;
	uint8_t sub_qos = 0, pub_qos = 0, pub_flag = 0, pub_dup = 0;
	uint16_t last_mid = 0, nrsubs = 0, nrpubs = 0, pub_mid = 0;
	uint32_t pub_payload_len = 0;
	int i = 0, j = 0;
	bool is_client_dup_sub = false;
	mosquitto__payload_uhpa pub_payload;
	enum mosquitto_msg_direction pub_dir;
	enum mosquitto_msg_state pub_state;
	struct mosquitto *client_context, *node;
	struct mosquitto_msg_store *stored = NULL;

	if(!context->is_node) return 1;
	//db__message_reconnect_reset();
	if(packet__read_string(&context->in_packet, &client_id)) return 1;

	HASH_FIND(hh_id,db->contexts_by_id, client_id, strlen(client_id), client_context);
	if(!client_context){
		mosquitto__free(client_id);
		return 1;
	}
	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] Receive SESSION RESP from node: %s for client: %s", context->id, client_id);
	if(packet__read_uint16(&context->in_packet, &last_mid)) return 1;/*where return 1??*/
	client_context->last_mid = last_mid;

	if(packet__read_uint16(&context->in_packet, &nrsubs)) return 1;

	if(nrsubs > 0){
		i = client_context->client_topic_count;
		client_context->client_topic_count += nrsubs;
		client_context->client_topics = mosquitto__realloc(client_context->client_topics, client_context->client_topic_count * (sizeof(struct client_topic_table*)));
	}

	/* handle subs */
	for(; i < client_context->client_topic_count; i ++){
		if(packet__read_string(&context->in_packet, &sub_topic)){
			return 1;
		}
		if(packet__read_byte(&context->in_packet, &sub_qos)){
			mosquitto__free(sub_topic);
			return 1;
		}
		log__printf(NULL, MOSQ_LOG_DEBUG, "\tSESSION RESP SUBs Total %d, (%d): topic:%s qos:%d(in)",
													nrsubs,i,sub_topic, sub_qos);
		 /* do not forward SUBSCRIBE for topic $SYS. */
		if(!IS_SYS_TOPIC(sub_topic)){
			struct topic_table *topic;
			HASH_FIND(hh, db->topics, sub_topic, strlen(sub_topic), topic);
			if(topic){
				is_client_dup_sub = false;
				for(j=0; j<client_context->client_topic_count; j++){
					if(client_context->client_topics[j] && !strcmp(client_context->client_topics[j]->topic_tbl->topic_payload, sub_topic))
					is_client_dup_sub = true;
				}
				if(!is_client_dup_sub)
					topic->ref_cnt++;
				else
					continue; /* this is a duplicate client subscription */
				client_context->is_db_dup_sub = true;
			}else{
				client_context->is_db_dup_sub = false;
				topic = mosquitto__malloc(sizeof(struct topic_table));
				topic->topic_payload = mosquitto__strdup(sub_topic);
				topic->ref_cnt = 1;
				HASH_ADD_KEYPTR(hh, db->topics, topic->topic_payload, strlen(topic->topic_payload), topic);
			}
			client_context->client_topics[i] = mosquitto__malloc(sizeof(struct client_topic_table));
			client_context->client_topics[i]->sub_qos = sub_qos; /* move remote client sub session to local client */
			client_context->client_topics[i]->topic_tbl = topic;
			if(!client_context->is_db_dup_sub){
				struct mosquitto_client_retain *cr = mosquitto__calloc(1, sizeof(struct mosquitto_client_retain));
				cr->client = client_context;
				cr->next = NULL;
				cr->retain_msgs = NULL;
				cr->expect_send_time = mosquitto_time() + 1;
				cr->sub_id = ++db->sub_id;
				client_context->last_sub_id = cr->sub_id;
				/* add a client retain msg list into db */
				if(!db->retain_list){
					db->retain_list = cr;
				}else{
					struct mosquitto_client_retain *tmp = db->retain_list;
					while(tmp->next){
						tmp = tmp->next;
					}
					tmp->next = cr;
				}
			} /* retained flag would be propagete from remote node */
			for(j=0; j<db->node_context_count && !client_context->is_db_dup_sub; j++){/* if this is a dupilicate db sub, then local must have the newest retained msg. */
				node = db->node_contexts[j];
				if(node && node->state == mosq_cs_connected){
					send__private_subscribe(node, NULL, sub_topic, sub_qos, client_context->id, db->sub_id);
				}
			}
		}
		sub__add(db, client_context, sub_topic, sub_qos, &db->subs);
		sub__retain_queue(db, client_context, sub_topic, sub_qos);
		mosquitto__free(sub_topic);
	}

	if(packet__read_uint16(&context->in_packet, &nrpubs)) return 1;
	/* handle pubs */
	for(i = 0; i < nrpubs; i ++){
		if(packet__read_string(&context->in_packet, &pub_topic)) return 1;
		if(packet__read_byte(&context->in_packet, &pub_flag)) return 1;
		pub_qos = pub_flag & 0x03;
		pub_dup = (pub_flag & 0x04) >> 2;
		pub_dir = (enum mosquitto_msg_direction)((pub_flag & 0x08) >> 3);
		pub_state = (enum mosquitto_msg_state)((pub_flag & 0xF0) >> 4);
		if(packet__read_uint16(&context->in_packet, &pub_mid)){
			mosquitto__free(pub_topic);
			return 1;
		}
		if(packet__read_uint32(&context->in_packet, &pub_payload_len)){
			mosquitto__free(pub_topic);
			return 1;
		}
		pub_payload.ptr = NULL;
		if(UHPA_ALLOC(pub_payload, pub_payload_len+1) == 0){
			mosquitto__free(pub_topic);
			return 1;
		}
		if(packet__read_bytes(&context->in_packet, UHPA_ACCESS(pub_payload, pub_payload_len), pub_payload_len)){
			mosquitto__free(pub_topic);
			return 1;
		}

		char *tmp_payload = mosquitto__malloc(pub_payload_len+1);
		memcpy(tmp_payload, pub_payload, pub_payload_len);
		tmp_payload[pub_payload_len] = '\0'; //\tSESSION RESP SUBS Total %d, (%d): topic:%s qos:%d(in)
		log__printf(NULL, MOSQ_LOG_DEBUG, "\tSESSION RESP PUBs Total %d, topic:%s flag(state|dir|dup|qos):0x%02X mid:%d payload:%s(in)",
													nrpubs, pub_topic, pub_flag, pub_mid, tmp_payload);
		mosquitto__free(tmp_payload);

		if(db__message_store(db, context->id, pub_mid, pub_topic, pub_qos, pub_payload_len, pub_payload, 0, &stored, 0)){
			UHPA_FREE(pub_payload, pub_payload_len);
			return 1;
		}
		if(db__message_session_pub_insert(db, client_context, pub_mid, pub_state, pub_dir, pub_dup, pub_qos, false, stored) == MOSQ_ERR_NOMEM){
			mosquitto__free(pub_topic);
			UHPA_FREE(pub_payload, pub_payload_len);
			return 1;
		}
	}
	db__message_reconnect_reset(db, client_context);

	return MOSQ_ERR_SUCCESS;
}
#endif
