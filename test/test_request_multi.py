#coding=utf-8

import requests
import json
import threading
from datetime import datetime

def send_request():
	url = 'http://localhost:8888'
	headers = {"Content-Type": "application/json"}
	json_obj = json.loads('{"tkey": "RequestTestTask", "param": {"name":"resec"}}')
	r = requests.post(url=url, headers=headers, json=json_obj)
	print(r.content)

if __name__ == "__main__":
	tlist = []
	for i in range(200):
		thread = threading.Thread(target=send_request)

		if i == 199:
			last = thread

		tlist.append(thread)

	start = datetime.now()

	for i in range(200):
		tlist[i].start()

	last.join()

	end = datetime.now()

	period = end - start

	print('period: ' + str(period))
	print(datetime.now())