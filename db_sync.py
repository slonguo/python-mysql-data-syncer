#!/usr/bin/env python
# -*- coding:utf-8 -*-

import ast
import json
import logging as log
import os
import re
import signal
import subprocess
import sys
import time
import traceback
from itertools import chain

import errno

import MySQLdb
import requests
import schedule
import yaml
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError
from elasticsearch.helpers import parallel_bulk
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.event import RotateEvent
from pymysqlreplication.row_event import (
	DeleteRowsEvent,
	UpdateRowsEvent,
	WriteRowsEvent
)


class ConfigException(Exception):
	"""Config exception base class."""

	def __str__(self):
		return self.__doc__


class MySQLConfigException(ConfigException):
	"""MySQL config exception."""


class ElasticsearchConfigException(ConfigException):
	"""Elasticsearch config exception."""


class SchemaException(Exception):
	"""Schema exception base class."""

	def __init__(self, db, table, data):
		self.db = db
		self.table = table
		self.data = data

	def __str__(self):
		return self.__doc__ + " [specifics] db:{}, table:{}, current params:{}".format(self.db, self.table, self.data)


class NoIdException(SchemaException):
	"""No valid id selected for Elasticsearch."""


class NoStampSpecifiedException(SchemaException):
	"""No valid stamp field specified for sync by query"""


def pretty_print_msg(data):
	if isinstance(data, dict):
		print(json.dumps(data, indent=4))
	else:
		print(data)

class DBSyncer(object):
	schema_d = {}
	log_file = None
	log_pos = None
	last_query_time = 0
	query_interval = 1  # minutes
	mysql_conf = {}
	es_conf = {}
	load_step = 5000

	def __init__(self):
		try:
			file_name = sys.argv[1] if len(sys.argv) > 1 else "river_conf.yaml"
			self.config = yaml.load(open(file_name, 'r').read())
		except Exception as e:
			pretty_print_msg("Error:%s" % (e,))
			exit(1)

		env = os.getenv(
			self.config['env']['env_var'],
			self.config['env']['env_default']
		)
		self.load_enabled = self.config.get('load_enabled', 0)
		self.sync_enabled = self.config.get('sync_enabled', 0)
		self.es_mapping_enabled = self.config.get('es_mapping_enabled', 0)
		self.data_load_type = self.config.get('data_load_type', 0)
		self.data_sync_type = self.config.get('data_sync_type', 0)
		self.load_step = self.config.get('load_step', 5000)
		self.load_step = min(max(5000, self.load_step), 40000)

		self._init_logging()

		mysql_configs = self.config.get('mysql_conf', [])
		for e in mysql_configs:
			if e.get('env', '') == env:
				self.mysql_conf = e

		if not self.mysql_conf:
			raise MySQLConfigException()

		self.mysql_conn_d = {
			k: self.mysql_conf[k] for k in
			[
				'host', 'user', 'passwd', 'db', 'port',
				'unix_socket', 'connect_timeout', 'charset',
			]
			if self.mysql_conf.get(k, None)
		}

		es_configs = self.config.get('es_conf', [])
		for e in es_configs:
			if e.get('env', '') == env:
				self.es_conf = e

		if self.es_mapping_enabled and not self.es_conf:
			raise ElasticsearchConfigException()

	@property
	def load_dump_open(self):
		return self.load_enabled == 1 and self.data_load_type == 1

	@property
	def load_query_open(self):
		return self.load_enabled == 1 and self.data_load_type == 2

	@property
	def sync_binlog_open(self):
		return self.sync_enabled == 1 and self.data_sync_type == 1

	@property
	def sync_query_open(self):
		return self.sync_enabled == 1 and self.data_sync_type == 2

	@property
	def es_host(self):
		return self.es_conf['addr']

	@property
	def es_bulk_chunk(self):
		return self.es_conf.get('bulk_chunk_size', 1000)

	def _init_logging(self):
		log_folder = self.config['logging']['folder']
		try:
			os.makedirs(log_folder)
		except OSError as exception:
			if exception.errno != errno.EEXIST:
				raise exception
		log_files = self.config['logging']['files']
		for log_file in log_files:
			open(os.path.join(log_folder, log_file), 'w').close()

		log_file = os.path.join(log_folder, self.config['logging']['log_file'])
		log.basicConfig(
			filename=log_file,
			level=log.DEBUG,
			format='[%(levelname)s] - [%(module)s]%(filename)s[line:%(lineno)d] - %(asctime)s %(message)s')
		self.logger = log.getLogger(__name__)

		# disable logging from other modules
		log.getLogger("requests").setLevel(log.WARNING)
		log.getLogger("urllib3").setLevel(log.WARNING)
		log.getLogger("elasticsearch").setLevel(log.WARNING)

		def exit_signal(*args):
			self.logger.info('Received exit signal:%s', args)
			sys.exit(0)

		signal.signal(signal.SIGINT, exit_signal)
		signal.signal(signal.SIGTERM, exit_signal)

	def _mysql_conn(self, db=None):
		conn_d = {k: v for k, v in self.mysql_conn_d.items()}
		if db:
			conn_d['db'] = db

		return MySQLdb.connect(**conn_d)

	def exec_sql(self, stmt, db=None, dict_cursor=False):
		if not stmt:
			return []
		conn = self._mysql_conn(db)
		with conn:
			cursor_class = MySQLdb.cursors.DictCursor if dict_cursor else MySQLdb.cursors.Cursor
			cur = conn.cursor(cursorclass=cursor_class)
			cur.execute(stmt)
			return cur.fetchall()

	def master_status(self, dict_cursor=True):
		stmt = "show master status"
		return self.exec_sql(stmt, dict_cursor=dict_cursor)

	def binary_logs(self, dict_cursor=True):
		stmt = "show binary logs"
		return self.exec_sql(stmt, dict_cursor=dict_cursor)

	def table_desc(self, table_name, db=None, dict_cursor=True):
		stmt = "desc %s`%s`" % ('`' + db + '`.' if db else '', table_name)
		return self.exec_sql(stmt, dict_cursor=dict_cursor)

	@staticmethod
	def _type_converter(mysql_field_type):
		# TODO add more types and type handlers here
		type_d = {
			'blob': 'string',
			'char': 'string',
			'varchar': 'string',
			'varbinary': 'string',
			'int': 'long',
			'int unsigned': 'long',
			'integer': 'long',
			'integer unsigned': 'long',
			'bigint': 'long',
			'bigint unsigned': 'long',
			'tinyint': 'long',
			'tinyint unsigned': 'long',
			'smallint': 'long',
			'smallint unsigned': 'long',
			'mediumint': 'long',
			'mediumint unsigned': 'long',
			'float': 'long',
			'double': 'long'
		}

		handler_d = {
			'integer': int,
			'long': int,
			'float': float,
			'double': float,
			'string': lambda x: x if not isinstance(x, str) else x.decode('utf-8'),
		}

		map_type = type_d.get(mysql_field_type, 'string')
		return {
			'type': map_type,
			'handler': handler_d[map_type]
		}

	def table_desc_d(self, table_name, db=None, ignore_fields=None):
		ignore_fields = ignore_fields or []
		rows = self.table_desc(table_name, db=db, dict_cursor=True)
		for row in rows:
			# int(10) unsigned => int unsigned
			# varchar(32) => varchar

			del row['Default']
			row['Type'] = re.sub("\(.*?\)", "", row['Type']).lower()

		return {row['Field']: row for row in rows if row['Field'] not in ignore_fields}

	def table_fields(self, table_name, db=None, ignore_fields=None):
		ignore_fields = ignore_fields or []
		rows = self.table_desc(table_name, db=db, dict_cursor=False)
		return [field[0] for field in rows if field[0] not in ignore_fields]

	@staticmethod
	def table_key(rule_mysql_d):
		return "`%s`.`%s`" % (
			rule_mysql_d['db'],
			re.sub("%(.*?)d", str(rule_mysql_d['table_cnt']), rule_mysql_d['table_fmt'])
		)

	@staticmethod
	def table_num(fmt, table):
		fmt = re.sub('(%.*?d)', '(.*)', fmt)
		result = re.search(fmt, table)
		if result:
			return int(result.group(1))
		else:
			return -1

	def _init_schema_item(self, db):
		if db and db not in self.schema_d:
			self.schema_d[db] = {
				key: {} for key in
				['table_fmt_map', 'tables_d',]
			}

	def _init_sync_conf(self):
		if self.sync_binlog_open:
			if self.load_enabled:
				binlog_d = self.master_status()[0]
				self.log_file = binlog_d['File']
				self.log_pos = binlog_d['Position']
				self._save_binlog_record()
			else:
				record_path = self.config['binlog_sync']['record_file']
				if os.path.isfile(record_path):
					with open(record_path, 'r') as f:
						record = yaml.load(f)
						if record:
							self.log_file = record.get('log_file')
							self.log_pos = record.get('log_pos')
		elif self.sync_query_open or self.load_query_open:
			record_path = self.config['query_sync']['record_file']
			if os.path.isfile(record_path):
				with open(record_path, 'r') as f:
					record = yaml.load(f)
					if record:
						self.last_query_time = record.get('last_query_time', 0)
						self.query_interval = record.get('query_interval', self.config['query_sync']['query_interval'])
					else:
						self.query_interval = self.config['query_sync']['query_interval']
			else:
				self.query_interval = self.config['query_sync']['query_interval']

	@staticmethod
	def now_t():
		return int(time.time())

	def _make_rule(self, rule_item):
		mysql_d = rule_item['mysql']
		es_d = rule_item['es']
		# key = self.table_key(mysql_d)
		db = mysql_d['db']
		table_fmt = mysql_d['table_fmt']
		table_cnt = mysql_d['table_cnt']
		self._init_schema_item(db)

		new_tab_name = re.sub("%(.*?)d", str(table_cnt), table_fmt)
		if table_cnt <= 1:
			table_fmt_map_d = {table_fmt: table_fmt}
			first_table = table_fmt
		else:
			table_fmt_map_d = {
				table_fmt % (i,): new_tab_name
				for i in range(0, table_cnt)
			}
			first_table = table_fmt % (0,)

		ignore_fields = mysql_d.get('ignore_fields', [])
		table_desc_d = self.table_desc_d(first_table, db=db, ignore_fields=ignore_fields)

		table_fields = self.table_fields(first_table, db=db)
		filtered_fields = [field for field in table_fields if field not in ignore_fields]
		unique_ids = mysql_d['unique_ids']
		if not unique_ids or not all(k in table_fields for k in unique_ids):
			raise NoIdException(db, table_fmt, unique_ids)

		stamp_param = mysql_d.get('stamp_param', '')
		if self.sync_query_open and (not stamp_param or stamp_param not in table_fields):
			raise NoStampSpecifiedException(db, table_fmt, stamp_param)

		self.schema_d[db]['table_fmt_map'].update(table_fmt_map_d)
		self.schema_d[db]['tables_d'][new_tab_name] = {
			'db': db,
			'key': new_tab_name,
			'fmt': table_fmt,
			'cnt': table_cnt,
			'desc': table_desc_d,
			'fields': filtered_fields,
			'orig_fields': table_fields,
			'tables': self.gen_tables(table_fmt, table_cnt),
			'unique_ids': unique_ids,
			'stamp_param': stamp_param,
			'doc_type': es_d['type'],
			'mapping': self._es_mapping_d(table_desc_d, es_d)
		}

	def parse(self):
		for item in self.config['rules']:
			try:
				self._make_rule(item)
			except (NoIdException, NoStampSpecifiedException), e:
				msg = "error in making rules:%s" % (e,)
				pretty_print_msg(msg)
				self.logger.error(msg)
				exit(1)

	def _es_mapping_d(self, table_d, es_d):
		# TODO
		# 1) add more settings to config file
		# 2) make properties more flexible

		def def_d(key):
			key_type = self._type_converter(table_d[key]['Type'])['type']
			return {
				"type": key_type
			} if key_type != "string" else {
				"type": key_type,
				"analyzer": "standard",
				"index": "analyzed"
			}

		return {
			'properties': {
				key: def_d(key) for key in table_d
			}
		}

	def _es_url(self, es_index, es_type, es_doc_id):
		return "%s/%s/%s/%s" % (self.es_host, es_index, es_type, es_doc_id)

	def _handle_sync(self, op, schema, src_table, d):
		table_key = self.schema_d[schema]['table_fmt_map'][src_table]
		table_info_d = self.schema_d[schema]['tables_d'][table_key]
		doc_type = table_info_d['doc_type']
		es_doc_id = self._gen_es_doc_id(table_info_d['unique_ids'], d)
		url = self._es_url(schema, doc_type, es_doc_id)
		resp = None
		if op in ("create", "update"):
			body = self._tweak_doc(table_info_d['desc'], d)
			resp = requests.put(url, json=body, timeout=10)
		elif op == "delete":
			resp = requests.delete(url, timeout=10)

		if resp and resp.status_code not in (200, 201):
			self.logger.error("op:%s code:%d, msg:%s", op, resp.status_code, resp.text)

	def _tweak_doc(self, desc_d, d, meta_d=None, need_convert=True):
		"""
		meta_d is needed in bulk api, containing at least _id and _op_type fields, eg:
		meta_d = {
			"_id": es_doc_id,
			"_op_type": "update",
			"_index": schema,
			"_type": doc_type,
		}
		"""

		new_d = {
			k: v if not need_convert else self._type_converter(desc_d[k]['Type'])['handler'](v)
			for k, v in d.items()
			if k in desc_d
		}

		if not meta_d:
			return new_d
		else:
			# create or update for bulk operations
			update_d = {
				"doc": new_d,
				"doc_as_upsert": True,
			}
			update_d.update(meta_d)
			return update_d

	@staticmethod
	def _gen_es_doc_id(unique_ids, d):
		return '_'.join([str(d[k]) for k in unique_ids])

	def dump_schema_d(self):
		pretty_print_msg(self.schema_d)

	def all_dbs_and_tables(self):
		dbs = self.schema_d.keys()
		tables = list(chain.from_iterable(self.schema_d[db]['table_fmt_map'].keys() for db in dbs))
		return dbs, tables

	def db_table_groups(self):
		return list(chain.from_iterable(v['tables_d'].values() for v in self.schema_d.values()))

	def _binlog_sync(self):
		if not self.sync_binlog_open:
			return

		dbs, tables = self.all_dbs_and_tables()
		stream = BinLogStreamReader(
			connection_settings=self.mysql_conn_d,
			server_id=self.mysql_conf['server_id'],
			only_events=[
				DeleteRowsEvent,
				WriteRowsEvent,
				UpdateRowsEvent,
				RotateEvent,
			],
			only_schemas=dbs,
			only_tables=tables,
			resume_stream=True,
			blocking=True,
			log_file=self.log_file,
			log_pos=self.log_pos)

		for be in stream:
			self.log_file = stream.log_file
			self.log_pos = stream.log_pos

			if isinstance(be, RotateEvent):
				self._save_binlog_record()
				continue

			for row in be.rows:
				schema = be.schema
				table = be.table
				if isinstance(be, WriteRowsEvent):
					self._handle_sync('create', schema, table, row['values'])
				elif isinstance(be, UpdateRowsEvent):
					self._handle_sync('update', schema, table, row['after_values'])
				elif isinstance(be, DeleteRowsEvent):
					self._handle_sync('delete', schema, table, row['values'])

		stream.close()

	def _query_sync(self):
		self.query_interval = max(self.query_interval, 1)
		schedule.every(self.query_interval).minutes.do(self._do_query)
		while True:
			schedule.run_pending()
			time.sleep(1)

	def _save_binlog_record(self):
		if self.sync_binlog_open:
			with open(self.config['binlog_sync']['record_file'], 'w') as f:
				self.logger.info(
					"sync log_file:%s  log_pos:%d",
					self.log_file,
					self.log_pos)
				yaml.safe_dump(
					{"log_file": self.log_file, "log_pos": self.log_pos},
					f,
					default_flow_style=False)

	def _save_stamp(self):
		if self.sync_query_open:
			with open(self.config['query_sync']['record_file'], 'w') as f:
				self.logger.info(
					"sync last_query_time:%d  query_interval:%d",
					self.last_query_time,
					self.query_interval)
				yaml.safe_dump(
					{"last_query_time": self.last_query_time, "query_interval": self.query_interval},
					f,
					default_flow_style=False
				)

	def _load_data(self):
		if self.load_dump_open:
			self._bulk_loader(self._dump_loader)
		elif self.load_query_open:
			self._do_query()
		else:
			self.logger.error(
				"invalid load config: enabled:%d, load type:%d",
				self.load_enabled,
				self.data_sync_type)

	def _do_query(self):
		current_t = self.now_t()
		if current_t >= self.last_query_time + self.query_interval * 60:
			self.logger.info(
				'[_do_query] now_t:%d, last_query_time:%d, query_interval:%d',
				current_t,
				self.last_query_time,
				self.query_interval
			)
			self._bulk_loader(self._query_loader)
			self.last_query_time = current_t
			self._save_stamp()

	def _query_loader(self):
		for table_d in self.db_table_groups():
			for table in table_d['tables']:
				start = 0
				while True:
					where_cond = " where %s > %d limit %d, %d " % (table_d['stamp_param'], self.last_query_time, start, self.load_step)
					stmt = "select * from `%s` " % (table,) + where_cond
					rows = self.exec_sql(stmt, db=table_d['db'], dict_cursor=True)
					if rows:
						yield table_d, rows
						start += self.load_step
					else:
						break

	def _dump_loader(self):
		cmd_conn = "mysqldump -u%(user)s -p%(passwd)s -h%(host)s -P%(port)d" % self.mysql_conf

		for table_d in self.db_table_groups():
			for table in table_d['tables']:
				cmd = chain(
					cmd_conn.split(' '),  # conn
					['-B', ], [table_d['db'], ],  # databases
					# ['--tables', ], table_d['tables'],  # tables
					['--tables', ], [table, ],  # tables
					['--no-create-info', '--compact', '--skip-opt', '--quick']  # extra options
				)

				sql_dump = subprocess.Popen(
					cmd,
					stdout=subprocess.PIPE,
					stderr=open(os.devnull, 'wb'),
					close_fds=True
				)

				elts = []
				cnt = 0
				for line in sql_dump.stdout:
					values = self.get_insert_values(line)
					if values:
						obj_tuple = ast.literal_eval(values)

						# some tables may have only one row
						# if not all(hasattr(obj, '__iter__') for obj in obj_tuple):
						if not hasattr(obj_tuple[0], '__iter__'):
							obj_tuple = (obj_tuple,)

						item_d_list = map(lambda x: dict(zip(table_d['orig_fields'], x)), obj_tuple)
						elts.extend(item_d_list)
						cnt += 1
						if len(elts) >= self.load_step:
							yield table_d, elts
							elts = []

				yield table_d, elts

	def _bulk_tweak_iter(self, table_d, elts):
		for elt in elts:
			doc_id = self._gen_es_doc_id(table_d['unique_ids'], elt)
			doc = self._tweak_doc(
				table_d['desc'],
				elt,
				meta_d={
					"_id": doc_id,
					"_op_type": "update",
					# "_index": table_d['db'],
					# "_type": table_d['doc_type']
				}
			)
			if doc:
				yield doc

	def _bulk_loader(self, handler):
		client = self._es_client()
		for table_d, elts in handler():
			if not elts:
				continue
			for ok, result in parallel_bulk(
					client,
					self._bulk_tweak_iter(table_d, elts),
					index=table_d['db'],
					doc_type=table_d['doc_type'],
					chunk_size=self.es_bulk_chunk
			):
				action, result = result.popitem()
				url = self._es_url(table_d['db'], table_d['doc_type'], result['_id'])
				if not ok:
					self.logger.error("failed to %s document %s: %s", action, url, result)

	@staticmethod
	def get_insert_values(line):
		if not line.startswith('INSERT INTO'):
			return ""
		return line.partition('` VALUES ')[2].strip().replace('NULL', "''")[:-1]

	@staticmethod
	def gen_tables(fmt, cnt):
		if not cnt or cnt <= 1:
			return [fmt, ]
		return [fmt % (i,) for i in range(0, cnt)]

	def _sync_data(self):
		if self.sync_binlog_open:
			self._binlog_sync()
		elif self.sync_query_open:
			self._query_sync()
		else:
			self.logger.error(
				"invalid sync config: enabled:%d, sync type:%d",
				self.sync_enabled,
				self.data_sync_type)

	def _es_client(self):
		return Elasticsearch(
			self.es_conf['addr'],
			sniffer_timeout=self.es_conf.get('timeout', 10)
		)

	def _init_es_mappings(self):
		if not self.es_mapping_enabled:
			return

		client = self._es_client()
		for k_schema, v_schema in self.schema_d.items():
			body = {
				"settings": {
					'number_of_shards': self.es_conf.get('shard_num', 1),
					'number_of_replicas': self.es_conf.get('replica_num', 0),
					'index': {
						'analysis': {
							'analyzer': {
								'standard': {
									'type': 'standard'
								}
							}
						}
					}
				},
			}

			try:
				client.indices.create(
					index=k_schema,
					body=body,
					update_all_types=True
				)
			except TransportError as e:
				if e.error == "index_already_exists_exception":
					pass
				else:
					raise

			for k_table, v_table in v_schema['tables_d'].items():
				# pretty_print_msg(v_table['mapping'])
				client.indices.put_mapping(
					v_table['doc_type'],
					v_table['mapping'],
					index=[k_schema, ],
					update_all_types=True
				)

	def _do_inits(self):
		self._init_sync_conf()
		self._init_es_mappings()

	def run(self):
		try:
			self.logger.info(
				"load_enabled:%d, sync_enabled:%d, load_type:%d, sync_type:%d ",
				self.load_enabled,
				self.sync_enabled,
				self.data_load_type,
				self.data_sync_type
			)

			self.parse()
			self._do_inits()
			self._load_data()
			self._sync_data()
		except Exception:
			self.logger.error(traceback.format_exc())
			exit(1)


def init_env():
	dir_path = os.path.dirname(os.path.realpath(__file__))
	os.chdir(dir_path)


if __name__ == '__main__':
	init_env()
	DBSyncer().run()
