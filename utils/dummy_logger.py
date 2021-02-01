from datetime import datetime

class DummyLogger():

	def warning(self, str_):
		print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} - {str_}")

	def info(self, str_):
		print(f"{datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')} - {str_}")
