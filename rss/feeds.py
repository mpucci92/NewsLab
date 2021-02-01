from apscheduler.schedulers.background import BlockingScheduler
from datetime import datetime, timezone
from collections import deque
from threading import Thread

from hashlib import sha256
from const import DIR
import feedparser
import socket
import sys, os
import json
import uuid

class Feeds(Thread):
	
	WINDOW = 1000

	def __init__(self, sources, feeds, sleep, logger):

		Thread.__init__(self)

		self.sleep = sleep
		self.logger = logger
		
		self.coords = deque([
			(source.strip(), feed.strip())
			for source, feed in zip(sources, feeds)
		])

		self.entries = []
		self.last_45 = {
			feed : []
			for _, feed in self.coords
		}

		socket.setdefaulttimeout(3)

	def run(self):

		job_defaults = {
			'coalesce': True,
			'max_instances': 1
		}
		self.blocker = BlockingScheduler(job_defaults = job_defaults)
		self.blocker.add_job(self.parse_feed, 'cron', second=f'*/{self.sleep}', id='parse_feed')
		
		self.blocker.start()

	def on_close(self):

		self.blocker.shutdown()
		self.join()
		
	def parse_feed(self):

		self.coords.rotate()
		self.source, self.feed = self.coords[0]
		
		try:
			response = feedparser.parse(self.feed)
		except Exception as e:
			self.logger.warning(f"Status,{self.source},{self.feed},{e}")
			return

		status = response.get('status', None)
		if not status:
			self.logger.warning(f"Status,{self.source},{self.feed},None")
			return

		if status != 200:
			self.logger.warning(f"Status,{self.source},{self.feed},{status}")
			return
		
		entries = response.get('entries', None)
		if not entries:
			self.logger.warning(f"Entries,{self.source},{self.feed},None")
			return

		for entry in entries:
			
			entry_str = json.dumps(entry).encode()
			entry_hash = sha256(entry_str).hexdigest()
			
			if entry_hash in self.last_45[self.feed]:
				break

			self.last_45[self.feed].append(entry_hash)
			self.last_45[self.feed] = self.last_45[self.feed][-self.WINDOW:]

			utc_now = datetime.now(tz=timezone.utc).strftime("%Y-%d-%m %H:%M:%S.%f")
			entry['oscrap_acquisition_datetime'] = utc_now
			entry['oscrap_source'] = self.source

			print(self.source)
			self.entries.append(entry)

		if len(self.entries) > 0:

			with open(f"{DIR}/news_data/{str(uuid.uuid4())}.txt", "w") as file:
				file.write(json.dumps(self.entries))
			self.entries = []
