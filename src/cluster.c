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

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>

#ifndef WIN32
#include <netdb.h>
#include <sys/socket.h>
#else
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include "config.h"

#include "mosquitto.h"
#include "mosquitto_broker.h"
#include "mosquitto_internal.h"
#include "net_mosq.h"
#include "memory_mosq.h"
#include "send_mosq.h"
#include "time_mosq.h"
#include "tls_mosq.h"
#include "util_mosq.h"
#include "will_mosq.h"

#ifdef WITH_CLUSTER
void node__disconnect(struct mosquitto_db *db, struct mosquitto *context)
{
	if(!context->is_node)
		return;
	if(context->state == mosq_cs_connected){
		nodes_disconn_times++;
		current_nodes--;
	}
	context->node->attemp_reconnect = mosquitto_time() + MOSQ_ERR_INTERVAL;
	context->node->handshaked = false;
	context->node->connrefused_interval = 2;
	context->node->hostunreach_interval = 2;

	context->ping_t = 0;
	context->state = mosq_cs_disconnected;
	//HASH_DELETE(hh_sock, db->contexts_by_sock, context);
	COMPAT_CLOSE(context->sock);
	context->sock = INVALID_SOCKET;
	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] node: %s down, do_disconnect now.", context->id);
	return;
}

int node__new(struct mosquitto_db *db, struct mosquitto__node *node)
{
	struct mosquitto *new_context = NULL;
	struct mosquitto **node_contexts;
	char *local_id;

	assert(db);
	assert(node);

	local_id = mosquitto__strdup(node->local_clientid);

	HASH_FIND(hh_id, db->contexts_by_id, local_id, strlen(local_id), new_context);
	if(new_context){
		/* (possible from persistent db) */
		_mosquitto_free(local_id);
	}else{
		/* id wasn't found, so generate a new context */
		new_context = mosquitto__calloc(1, sizeof(struct mosquitto));
		if(!new_context){
			_mosquitto_free(local_id);
			log__printf(NULL, MOSQ_LOG_ERR, "[CLUSTER] ERROR: out of memory while creating node: %s.", node->name);
			return MOSQ_ERR_NOMEM;
		}
		new_context->state = mosq_cs_new;
		new_context->sock = INVALID_SOCKET;
		new_context->last_msg_in = 0;
		new_context->next_msg_out = mosquitto_time() + MOSQ_CLUSTER_KEEPALIVE;
		new_context->clean_session = false;
		new_context->disconnect_t = 0;
		new_context->id = NULL;
		new_context->last_mid = 0;
		new_context->will = NULL;
		new_context->acl_list = NULL;
		new_context->listener = NULL;
		new_context->is_bridge = false;
		new_context->is_node = true;
		new_context->is_peer = false;
		
		new_context->save_subs = false;
		new_context->is_sys_topic = true;
		new_context->is_db_dup_sub = true;
		new_context->last_sub_id = 0;
		new_context->client_topic_count = 0;
		new_context->remote_time_offset = 0;
		new_context->last_sub_client_id = NULL;
		new_context->db = db;
		new_context->client_topics = NULL;
		
		new_context->in_packet.payload = NULL;
		packet__cleanup(&new_context->in_packet);
		new_context->out_packet = NULL;
		new_context->current_out_packet = NULL;
		new_context->address = NULL;
		new_context->bridge = NULL;
		new_context->msg_count = 0;
		new_context->msg_count12 = 0;
#ifdef WITH_TLS
		new_context->ssl = NULL;
#endif
		new_context->id = local_id;
		HASH_ADD_KEYPTR(hh_id, db->contexts_by_id, new_context->id, strlen(new_context->id), new_context);
	}
	new_context->sock = INVALID_SOCKET;
	new_context->node = node;
	new_context->keepalive = MOSQ_CLUSTER_KEEPALIVE;
	new_context->username = new_context->node->remote_username;
	new_context->password = new_context->node->remote_password;

#ifdef WITH_TLS
	new_context->tls_cafile = new_context->node->tls_cafile;
	new_context->tls_capath = new_context->node->tls_capath;
	new_context->tls_certfile = new_context->node->tls_certfile;
	new_context->tls_keyfile = new_context->node->tls_keyfile;
	new_context->tls_cert_reqs = SSL_VERIFY_PEER;
	new_context->tls_version = new_context->node->tls_version;
	new_context->tls_insecure = new_context->node->tls_insecure;
#ifdef REAL_WITH_TLS_PSK
	new_context->tls_psk_identity = new_context->node->tls_psk_identity;
	new_context->tls_psk = new_context->node->tls_psk;
#endif
#endif
	new_context->node->context = new_context;
	new_context->protocol = node->protocol_version;
	node->handshaked = false;
	node->hostunreach_interval = 2;
	node->connrefused_interval = 2;
	node->attemp_reconnect = 0;
	node->check_handshake = 0;
	log__printf(NULL, MOSQ_LOG_DEBUG, "[CLUSTER] New node: %s context created.", node->name);

	node_contexts = mosquitto__realloc(db->node_contexts, (db->node_context_count+1) * sizeof(struct mosquitto *));
	if(node_contexts){
		db->node_contexts = node_contexts;
		db->node_context_count++;
		db->node_contexts[db->node_context_count-1] = new_context;
		return MOSQ_ERR_SUCCESS;
	}else{
		log__printf(NULL, MOSQ_LOG_ERR, "[CLUSTER] ERROR: out of memory while creating node: %s.", node->name);
		return MOSQ_ERR_NOMEM;
	}
}

void node__cleanup(struct mosquitto_db *db, struct mosquitto *context)
{
	if(!context->is_node) return;

	int i;

	for(i=0; i<db->node_context_count; i++){
		if(db->node_contexts[i] == context){
			db->node_contexts[i] = NULL;
		}
	}
	mosquitto__free(context->node->local_clientid);
	context->node->local_clientid = NULL;

	mosquitto__free(context->node->local_username);
	context->node->local_username = NULL;
	
	mosquitto__free(context->node->local_password);
	context->node->local_password = NULL;

	if(context->node->remote_clientid != context->id){
		mosquitto__free(context->node->remote_clientid);
	}
	context->node->remote_clientid = NULL;

	if(context->node->remote_username != context->username){
		mosquitto__free(context->node->remote_username);
	}
	context->node->remote_username = NULL;

	if(context->node->remote_password != context->password){
		mosquitto__free(context->node->remote_password);
	}
	context->node->remote_password = NULL;
}

int node__try_connect(mosquitto_db *db, mosquitto *context)
{
	int rc;
	time_t now = mosquitto_time();
	if(!context->is_node)
		return MOSQ_ERR_INVAL;
	mosquitto__node *node = context->node;
	rc = net__try_connect(context, node->address, node->port, &context->sock, NULL, false);
	if(rc == 0){
		context->state = mosq_cs_new;
		log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] Success in handshake with node: %s immediately.", node->name);
		node->handshaked = true;
		HASH_ADD(hh_sock, db->contexts_by_sock, sock, sizeof(context->sock), context);
		send__connect(context, context->keepalive, false);
		context->next_pingreq = now;
	}else if(rc < 0){
		context->state = mosq_cs_connect_pending;
		log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] Current cannot handshake with node: %s. reason:%s.", node->name, strerror(errno));
		node->handshaked = false;
		node->check_handshake = now + MOSQ_CHECKCONN_INTERVAL;
	}else{
		context->state = mosq_cs_disconnected;
		assert(context->sock == INVALID_SOCKET);
		node->handshaked = false;
		log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] Error in handshake with node: %s. reason:%s.", node->name, strerror(errno));
	}
	return rc;
}

int node__check_connect(struct mosquitto_db *db, struct mosquitto *context)
{
	int err, rc, reconnect_interval;
	time_t now = mosquitto_time();
	mosquitto__node *node = context->node;
	socklen_t errlen = sizeof(err);/* getsockopt return -1 on solaris and return 0 on other os. */
	rc = getsockopt(context->sock, SOL_SOCKET, SO_ERROR, &err, &errlen);
	if(rc == 0 && err == 0){
		context->state = mosq_cs_new;
		log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] Finally handshake with node: %s success.", node->name);
		node->handshaked = true;
		HASH_ADD(hh_sock, db->contexts_by_sock, sock, sizeof(context->sock), context);
	    //log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] Will send CONNECT with node: %s.", peer_context->node->name);
		send__connect(context, context->keepalive, true);
		context->next_pingreq = now;
		node->connrefused_interval = 2;
		node->hostunreach_interval = 2;
		return MOSQ_ERR_SUCCESS;
	}else{
		context->state = mosq_cs_disconnected;
		//log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] Still cannot handshake with node: %s. reason:%d. will close sockfd.", peer_context->node->name, err);
		node->handshaked = false;
		COMPAT_CLOSE(context->sock);
		context->sock = INVALID_SOCKET;
		switch(err){
			case EINPROGRESS:
				log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] node: %s is busy, will reconnect later after %d seconds..", node->name, MOSQ_EINPROGRESS_INTERVAL);
				node->attemp_reconnect = now + MOSQ_EINPROGRESS_INTERVAL;
				context->state = mosq_cs_connect_pending;
				return MOSQ_ERR_CONN_PENDING;
			case EHOSTUNREACH:
				log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] node: %s OS maybe down or network unavailable, will reconnect later after %d seconds..", node->name, node->hostunreach_interval);
				node->attemp_reconnect = now + node->hostunreach_interval;
				if(node->hostunreach_interval*2 < MOSQ_NO_ROUTE_INTERVAL_MAX)
					reconnect_interval = node->hostunreach_interval*2;
				else
					reconnect_interval = MOSQ_NO_ROUTE_INTERVAL_MAX;
				node->hostunreach_interval = reconnect_interval;
				return MOSQ_ERR_CONN_LOST;
			case ECONNREFUSED:
				log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] node: %s service maybe down, will reconnect later after %d seconds..", node->name, node->connrefused_interval);
				node->attemp_reconnect = now + node->connrefused_interval;
				if(node->connrefused_interval*2 < MOSQ_ECONNREFUSED_INTERVAL_MAX)
					reconnect_interval = node->connrefused_interval*2;
				else
					reconnect_interval = MOSQ_ECONNREFUSED_INTERVAL_MAX;
				node->connrefused_interval = reconnect_interval;
				return MOSQ_ERR_CONN_REFUSED;
			default:
				log__printf(NULL, MOSQ_LOG_ERR, "[HANDSHAKE] unknow reason while connect with node: %s, will try to reconnect after %d seconds..", node->name, MOSQ_ERR_INTERVAL);
				node->attemp_reconnect = now + MOSQ_ERR_INTERVAL;
				return MOSQ_ERR_NO_CONN;
		}
	}
}

void node__packet_cleanup(struct mosquitto *context)
{
	struct mosquitto__packet *packet;
	if(!context) return;

	if(context->current_out_packet){
		packet__cleanup(context->current_out_packet);
		mosquitto__free(context->current_out_packet);
		context->current_out_packet = NULL;
	}
    while(context->out_packet){
		packet__cleanup(context->out_packet);
		packet = context->out_packet;
		context->out_packet = context->out_packet->next;
		mosquitto__free(packet);
	}
	context->out_packet = NULL;
	context->out_packet_last = NULL;

	packet__cleanup(&(context->in_packet));
}

int mosquitto_handle_retain(struct mosquitto_db *db)
{
	time_t now;
	struct mosquitto_client_retain *cr = NULL, *prev_cr = NULL, *tmp_cr = NULL;

	now = mosquitto_time();
	cr = db->retain_list;
    while(cr && (now >= cr->expect_send_time)){
		if(cr == db->retain_list){
			db->retain_list = db->retain_list->next;
		}else if(cr == db->retain_list->next){
			prev_cr = db->retain_list;
		}
		tmp_cr = cr;
		cr = cr->next;
		if(prev_cr) prev_cr->next = cr;
		while(tmp_cr->retain_msgs){
			db__message_insert_to_inflight(db, tmp_cr->client, tmp_cr->retain_msgs);
			tmp_cr->retain_msgs = tmp_cr->retain_msgs->next;
		}
		mosquitto__free(tmp_cr);
	}
	return MOSQ_ERR_SUCCESS;
}

int mosquitto_cluster_init(struct mosquitto_db *db, struct mosquitto *context)
{
	int notification_topic_len, i;
	char notification_payload;
	char *notification_topic;
	char **topic_arr;

	if(context->is_node){/* keep cluster status */
		notification_topic_len = strlen(context->node->remote_clientid) + strlen("$SYS/broker/connection/cluster//state");
		notification_topic = _mosquitto_malloc(sizeof(char)*(notification_topic_len+1));
		if(!notification_topic) return MOSQ_ERR_NOMEM;

		snprintf(notification_topic, notification_topic_len+1, "$SYS/broker/connection/cluster/%s/state", context->node->remote_clientid);
		notification_payload = '1';
		db__messages_easy_queue(db, context, notification_topic, 1, 1, &notification_payload, 1);
		mosquitto__free(notification_topic);
	}
	if(context->is_node){/* subscribe topic which has been subscribed only by local CLIENT */
		struct topic_table *topic, *tmp_topic;
		i = 0;
		topic_arr = (char**)_mosquitto_malloc(MULTI_SUB_MAX_TOPICS * sizeof(char*));
		HASH_ITER(hh, db->topics, topic, tmp_topic){/* FIXME: current only support topic SUBSCRIBE one by one */
			assert(topic->topic_payload);
			topic_arr[i++] = topic->topic_payload;
			if(i == MULTI_SUB_MAX_TOPICS){
				if(send__multi_subscribes(context, NULL, topic_arr, i)){
					mosquitto__free(topic_arr);
					return 1;
				}
				i = 0;
			}
		}
		if(i > 0 && i < MULTI_SUB_MAX_TOPICS){
			if(send__multi_subscribes(context, NULL, topic_arr, i)){
				mosquitto__free(topic_arr);
				return 1;
			}
		}
		mosquitto__free(topic_arr);
	}
	return MOSQ_ERR_SUCCESS;
}

int mosquitto_cluster_subscribe(struct mosquitto_db *db, struct mosquitto *context, char *sub, uint8_t qos)
{
	int i;
	bool is_client_dup_sub = false;
	struct topic_table *topic_iterm = NULL;
	struct client_topic_table **new_client_topics = NULL, *tmp_client_topic = NULL;
	struct mosquitto *node;

	if(context->is_peer || IS_SYS_TOPIC(sub))
		return MOSQ_ERR_SUCCESS;
	context->is_db_dup_sub = false;
	context->is_sys_topic = false;

	/* for clients */
	HASH_FIND(hh, db->topics, sub, strlen(sub), topic_iterm); /* find this topic inside DB */
	if(topic_iterm){
		topic_iterm->ref_cnt++;
		context->is_db_dup_sub = true;
	}else{/* add this new sub to db */
		topic_iterm = mosquitto__malloc(sizeof(struct topic_table));
		if(!topic_iterm){
			return MOSQ_ERR_NOMEM;
		}
		topic_iterm->topic_payload = mosquitto__strdup(sub);
		topic_iterm->ref_cnt = 1;
		HASH_ADD_KEYPTR(hh, db->topics, topic_iterm->topic_payload, strlen(topic_iterm->topic_payload), topic_iterm);
	}
	for(i=0; i<context->client_topic_count; i++){ /* check for duplicate client sub */
		if(context->client_topics[i] && 
			!strcmp(context->client_topics[i]->topic_tbl->topic_payload, topic_iterm->topic_payload)){
			context->client_topics[i]->sub_qos = qos; /* update QoS */
			assert(context->is_db_dup_sub); /* a duplicate client sub must be in the db topics */
			is_client_dup_sub = true;
			break;
		}
	}

	if(!context->is_db_dup_sub) assert(!is_client_dup_sub);

	log__printf(NULL, MOSQ_LOG_INFO, "Client %s subscribe for topic: %s. This sub is %s for local broker, %s for client.", context->id, sub, context->is_db_dup_sub?"stale":"fresh", is_client_dup_sub?"stale":"fresh");

	for(i=0; i<context->client_topic_count && !is_client_dup_sub; i++){ /* add this topic into client_topic */
		if(!context->client_topics[i]){
			context->client_topics[i]->topic_tbl = topic_iterm;
			context->client_topics[i]->sub_qos = qos;
			break;
		}
	}
	if(!is_client_dup_sub && i == context->client_topic_count){/* alloc new room for this fresh sub */
		new_client_topics = mosquitto__realloc(context->client_topics, sizeof(struct client_topic_table *)*(context->client_topic_count + 1));
		tmp_client_topic = _mosquitto_malloc(sizeof(struct client_topic_table));
		if(!new_client_topics || !tmp_client_topic){
			HASH_DELETE(hh,db->topics,topic_iterm);
			mosquitto__free(topic_iterm->topic_payload);
			mosquitto__free(topic_iterm);
			return MOSQ_ERR_NOMEM;
		}
		tmp_client_topic->sub_qos = qos;
		tmp_client_topic->topic_tbl = topic_iterm;
		context->client_topics = new_client_topics;
		context->client_topics[context->client_topic_count++] = tmp_client_topic;
	}
	 /* do we need to take a retain room for any of the client subs? */
	if(!context->is_db_dup_sub){/* for duplicated sub, local should have the retain msg, so don't take care of them. */
		struct mosquitto_client_retain *cr = mosquitto__calloc(1, sizeof(struct mosquitto_client_retain));
		cr->client = context;
		cr->next = NULL;
		cr->retain_msgs = NULL;
		cr->expect_send_time = mosquitto_time() + 1;
		cr->sub_id = ++db->sub_id;
		context->last_sub_id = cr->sub_id;

		if(!db->retain_list){ /* add a client retain msg list into db */
			db->retain_list = cr;
		}else{
			struct mosquitto_client_retain *tmp = db->retain_list;
			while(tmp->next){
				tmp = tmp->next;
			}
			tmp->next = cr;
		}
	}
	/* retained flag would be propagete from remote node */
	for(i=0; i<db->node_context_count && !context->is_db_dup_sub; i++){/* if this is a dupilicate db sub, then local must have the newest retained msg. */
		node = db->node_contexts[i];
		if(node && node->state == mosq_cs_connected){
			send__private_subscribe(node, NULL, sub, qos, context->id, db->sub_id);
		}
	}

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_cluster_unsubscribe(struct mosquitto_db *db, struct mosquitto *context, char *sub)
{
	if(context->is_peer || IS_SYS_TOPIC(sub))
		return MOSQ_ERR_SUCCESS;

	int i;
	bool is_dup_unsub = true;
	struct mosquitto *node;
	struct topic_table *topic;
	struct topic_table *client_topic;

	/* check for duplicate unsubscribe */
	for(i=0; i<context->client_topic_count; i++){
		if(context->client_topics[i] && !strcmp(sub, context->client_topics[i]->topic_tbl->topic_payload)){
			client_topic = context->client_topics[i]->topic_tbl;
			mosquitto__free(context->client_topics[i]);
			context->client_topics[i] = NULL;
			is_dup_unsub = false;
			break;
		}
	}

	if(!is_dup_unsub){
		HASH_FIND(hh, db->topics, sub, strlen(sub), topic);
		if(topic){ /* this topic indeed subscribed before. */
			assert(client_topic == topic);
			topic->ref_cnt--;
			if(topic->ref_cnt == 0){
				log__printf(NULL, MOSQ_LOG_INFO, "Client %s unsubscribe for topic: %s. This unsub is valid to unsub in cluster, will send UNSUBSCRIBE to other nodes.", context->id, sub);

				for(i=0; i< db->node_context_count; i++){
					node = db->node_contexts[i];
					if(node && node->is_node && !node->is_peer && node->state == mosq_cs_connected && node->sock != INVALID_SOCKET){
						send__unsubscribe(node, NULL, sub);
					}
				}
				HASH_DELETE(hh, db->topics, topic);
				if(topic->topic_payload)
					mosquitto__free(topic->topic_payload);
				mosquitto__free(topic);
			}
		}
	}

	return MOSQ_ERR_SUCCESS;
}

int mosquitto_cluster_client_disconnect(struct mosquitto_db *db, struct mosquitto *context)
{
	int i = 0, j = 0, k = 0;
	char *unsub_topic = NULL;
	struct topic_table *topic_tbl = NULL, *tmp_topic_tbl = NULL;
	struct client_topic_table *client_topic = NULL;
	struct mosquitto *node = NULL;
	if(context->is_node || context->is_peer || context->save_subs || !context->clean_session)
		return MOSQ_ERR_SUCCESS;
	char **topic_arr = mosquitto__malloc(MULTI_SUB_MAX_TOPICS * sizeof(char*));
	if(!topic_arr)
		return MOSQ_ERR_NOMEM;
	for(i = 0; i < context->client_topic_count; i++){
		client_topic = context->client_topics[i];
		if(!client_topic || !strncmp(client_topic->topic_tbl->topic_payload,"$SYS",4)) continue;

		unsub_topic = client_topic->topic_tbl->topic_payload;

		log__printf(NULL, MOSQ_LOG_DEBUG, "[CLIENT_TOPIC_TABLE] Client %s disconnecting.. total(%d), subscribed topic(%d): %s.", context->id, context->client_topic_count, i, unsub_topic);
		HASH_ITER(hh,db->topics, topic_tbl, tmp_topic_tbl){
			log__printf(NULL, MOSQ_LOG_DEBUG, "[DB_TOPIC_TABLE] iter:%d topic: %s(ref_cnt:%d).", i, topic_tbl->topic_payload, topic_tbl->ref_cnt);
		}

		topic_tbl = NULL;
		HASH_FIND(hh, db->topics, unsub_topic, strlen(unsub_topic), topic_tbl);
		if(topic_tbl){ /* delete this topic from topic table, pay attention to illegal UNSUBSCRIBE. */
			assert(topic_tbl == client_topic->topic_tbl);
			topic_tbl->ref_cnt--;
			if(topic_tbl->ref_cnt == 0){
				if(topic_tbl->topic_payload){
					topic_arr[k++] = topic_tbl->topic_payload;
					log__printf(NULL, MOSQ_LOG_DEBUG, "after k++, k: %d", k);
				}
				HASH_DELETE(hh, db->topics, topic_tbl);
				mosquitto__free(topic_tbl);
			}
		}else{
			log__printf(NULL, MOSQ_LOG_DEBUG, "client sub topic:%s not found in db",unsub_topic);
		}
		mosquitto__free(client_topic);
		if(k == MULTI_SUB_MAX_TOPICS){
			for(j=0; j < db->node_context_count; j++){
				node = db->node_contexts[j];
				if(node && node->is_node && !node->is_peer && node->state == mosq_cs_connected && node->sock != INVALID_SOCKET){
					send__multi_unsubscribe(node, NULL, topic_arr, MULTI_SUB_MAX_TOPICS);/* will memcpy topic_payload inside */
				}
			}
			for(j=0; j<MULTI_SUB_MAX_TOPICS; j++){
				mosquitto__free(topic_arr[j]);
			}
			k = 0;
		}
	}
	if(k>0 && k<MULTI_SUB_MAX_TOPICS){
		log__printf(NULL, MOSQ_LOG_DEBUG, "will send multi unsub, k: %d", k);
		for(j=0; j < db->node_context_count; j++){
			node = db->node_contexts[j];
			if(node && node->is_node && !node->is_peer && node->state == mosq_cs_connected && node->sock != INVALID_SOCKET){
				send__multi_unsubscribe(node, NULL, topic_arr, k);/* will memcpy topic_payload inside */
			}
		}
		for(j=0; j<k; j++){
			_mosquitto_free(topic_arr[j]);
		}
	}
	topic_tbl = NULL;
	tmp_topic_tbl = NULL;
	HASH_ITER(hh,db->topics, topic_tbl, tmp_topic_tbl){
		log__printf(NULL, MOSQ_LOG_DEBUG, "[DB_TOPIC_TABLE] after send multi ubsub, db->topics: %s(ref_cnt:%d).", topic_tbl->topic_payload, topic_tbl->ref_cnt);
	}
	mosquitto__free(topic_arr);
	mosquitto__free(context->client_topics);
	return MOSQ_ERR_SUCCESS;
}
#endif
