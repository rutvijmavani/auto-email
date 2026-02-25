"""
This call sends a message to the given recipient with attachment.
"""
from mailjet_rest import Client
import os
from dotenv import load_dotenv

load_dotenv()

api_key = os.getenv('MJ_APIKEY_PUBLIC')
api_secret = os.getenv('MJ_APIKEY_PRIVATE')
mailjet = Client(auth=(api_key, api_secret), version='v3.1')
data = {
  'Messages': [
				{
						"From": {
								"Email": "rutvijmavani@gmail.com",
								"Name": "Rutvij Mavani"
						},
						"To": [
								{
										"Email": "rutvijmavani22@gmail.com",
										"Name": "passenger 1"
								}
						],
						"Subject": "Your email flight plan!",
						"TextPart": "Dear passenger 1, welcome to Mailjet! May the delivery force be with you!",
						"HTMLPart": "<h3>Dear passenger 1, welcome to <a href=\"https://www.mailjet.com/\">Mailjet</a>!</h3><br />May the delivery force be with you!",
						"Attachments": [
								{
										"ContentType": "text/plain",
										"Filename": "test.txt",
										"Base64Content": "VGhpcyBpcyB5b3VyIGF0dGFjaGVkIGZpbGUhISEK"
								}
						]
				}
		]
}
result = mailjet.send.create(data=data)

print(result.status_code)
print(result.json())