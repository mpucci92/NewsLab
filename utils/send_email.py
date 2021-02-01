from dummy_logger import DummyLogger
from mailjet_rest import Client
import base64
import sys,os

def encode_text(filename, filepath):

	with open(f"{filepath}/{filename}", "r") as file:
		content = file.read()

	content = content.encode()
	content = base64.b64encode(content)
	content = content.decode()

	return {
		"ContentType" : "text/plain",
		"Filename" : filename,
		"Base64Content" : content
	}

def encode_zip(filename, filepath):

	with open(f"{filepath}/{filename}.b64", "wb+") as b64_file:
		with open(f"{filepath}/{filename}", "rb") as zip_file:
			base64.encode(zip_file, b64_file)
		b64_file.seek(0)
		content = b64_file.read()
	content = content.decode()
	
	os.remove(f"{filepath}/{filename}.b64")

	return {
		"ContentType" : "application/zip",
		"Filename" : filename,
		"Base64Content" : content
	}

def send_email(config, subject, body, attachments, logger=None):

	if not logger:
		logger = DummyLogger()

	max_tries = 5
	email_attempts = 0

	while email_attempts < max_tries:

		try:

			api_public_key = config['mailjet_public_key']
			api_private_key = config['mailjet_private_key']
			
			client = Client(auth=(api_public_key, api_private_key), version='v3.1')

			b64_attachments = []
			for attachment in attachments:
				
				filename = attachment['filename']
				filepath = attachment['filepath']

				if attachment['ContentType'] == "plain/text":
					
					b64_attachments.append(encode_text(filename, filepath))

				elif attachment['ContentType'] == "application/zip":
					
					encoded_zip = encode_zip(filename, filepath)
					
					filesize = sys.getsizeof(encoded_zip['Base64Content'])
					filesize /= 1_000_000

					if filesize < 15:
						b64_attachments.append(encoded_zip)

			data = {
				"Messages" : [
					{
						"From" : config['mailjet_sender'],
						"To" : config['mailjet_recipients'],
						"Subject" : subject,
						"HTMLPart" : body,
						"Attachments" : b64_attachments
					},
				]
			}
			
			result = client.send.create(data=data)
			status, response = result.status_code, result.json()

			if status == 200:
				break
			else:
				logger.warning(f"Email Status Error. {status} - {response}")

		except Exception as e:

			logger.warning(f"Emailing Attempt Error. {e}")

		email_attempts += 1

	if email_attempts >= max_tries:
		raise Exception("Emailing Failure.")
