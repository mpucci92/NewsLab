from const import DIR, logger, date_today
from joblib import delayed, Parallel
from feeds import Feeds
import pandas as pd
import sys, os
import joblib
import signal

###################################################################################################

feeds = pd.read_csv(f"{DIR}/data/rss.csv").iloc[:, :2]
feeds.columns = ['source', 'feed']

with open(f'{DIR}/data/groups.pkl', 'rb') as file:
	groups = joblib.load(file)

###################################################################################################

def parallel_job(job_id, parallel_group):

	logger.info(f"RSS,Job,PID,{os.getpid()}")

	def on_close():

		for group in parallel_group:

			feed_threads[group].on_close()
			logger.info(f"RSS,Thread,Closed,{job_id} - {group}")

	def sigterm_handler(signal_number, frame):

		logger.info(f"RSS,Job,SIGTERM,{os.getpid()}")
		on_close()

	signal.signal(signal.SIGTERM, sigterm_handler)
	os.system(f"touch {DIR}/pids/{os.getpid()}")

	###############################################################################################

	try:
		
		feed_threads = {}

		for i, group in enumerate(parallel_group):
			
			group, sleep = group, groups[group]
			group_coords = feeds[feeds.source.isin(group)]
			
			feed_threads[group] = Feeds(
				sources = group_coords.source.values,
				feeds = group_coords.feed.values,
				sleep = sleep,
				logger = logger
			)

			feed_threads[group].start()

			logger.info(f"RSS,Thread,Initiated,{job_id} - {group}")

	except Exception as e:

		logger.warning(f"RSS,Thread,Error,{job_id} - {e}")

		on_close()
		
		raise Exception(f"RSS,Job,Terminated,{job_id} - {e}")

def main():

	logger.info(f"RSS,Job,Initated,{date_today}")

	for file in os.listdir(f"{DIR}/pids"):
		
		if file == ".gitignore":
			continue
		
		os.remove(f"{DIR}/pids/{file}")

	os.system(f"touch {DIR}/pids/{os.getpid()}")
	
	group_keys = list(groups.keys())
	parallel_groups = [group_keys[0::2], group_keys[1::2]]

	try:

		Parallel(n_jobs=2)(
			delayed(parallel_job)(job_id, parallel_group)
			for job_id, parallel_group in enumerate(parallel_groups)
		)

	except Exception as e:

		logger.warning(e)

if __name__ == '__main__':

	try:
		main()
	except Exception as e:
		logger.warning(e)