#!/usr/bin/env python
# -*- coding:utf-8 -*

import json
import os

import requests
from elasticsearch import Elasticsearch
import yaml
import sys


def pretty_print_msg(data):
	if isinstance(data, dict):
		print(json.dumps(data, indent=4))
	else:
		print(data)


class QuerySamples(object):
	def __init__(self):
		try:
			file_name = sys.argv[1] if len(sys.argv) > 1 else "conf.yaml"
			self.config = yaml.load(open(file_name, 'r').read())
		except Exception as e:
			pretty_print_msg("Error:%s" % (e,))
			exit(1)

		env = os.getenv(
			self.config['env']['env_var'],
			self.config['env']['env_default']
		)

		mysql_configs = self.config.get('mysql_conf', [])
		for e in mysql_configs:
			if e.get('env', '') == env:
				self.mysql_conf = e

		es_configs = self.config.get('es_conf', [])
		for e in es_configs:
			if e.get('env', '') == env:
				self.es_conf = e

		self.go_addr = self.config['go_conf']['addr'][env]

	def run_es_query(self):
		client = Elasticsearch(self.es_conf['addr'])
		req_bodies = [
			{
				"index": "item_db",
				"type": "order_tab",
				"body": {
					"query": {
						"bool": {
							"must": {
								"match": {
									"sku_list": "S00002"
								},
								"match": {
									"seller_email": "gmail.com"
								}
							}
						}
					},
					"from": 0,
					"size": 2
				},
			},
		]

		for req in req_bodies:
			result = client.search(
				index=req['index'],
				doc_type=req['type'],
				body=req['body']
			)
			pretty_print_msg(result)
			print(result["hits"]["total"])

	def run_go_query(self):
		print(self.go_addr)
		resp = requests.post(url=self.go_addr + '/item/skus', data={'key': 'Promotion'})
		pretty_print_msg(resp.json())

	def run(self):
		#self.run_es_query()
		self.run_go_query()
		pass


def init_env():
	dir_path = os.path.dirname(os.path.realpath(__file__))
	os.chdir(dir_path)


if __name__ == '__main__':
	init_env()
	QuerySamples().run()
	pass
