import requests
import json
import os

with open("./metric.json", 'r', encoding='utf-8') as load_f:
    load_dict = json.load(load_f)
id = 0
result = []
for i in load_dict[0]['rows']:
    metric_id = i[1]
    url = "https://kensho2-api.azurewebsites.net/smartalert/get_detect_configs_by_metric"
    # payload =" {\"metricGuid\": \""+str(metric_id)+"'\"}"
    # payload ="{\"metricGuid\": \"6d8d16bb-b830-48e5-aefb-5084a67db5ce\"}"
    payload = "{\"metricGuid\": \"" + str(metric_id) + "\"}"
    headers = {
        'x-api-key': '382575d0-c127-41d5-bb65-af43d386e57d',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=payload)
    response = response.json()  # .encode('utf8')
    print(response)
    # if response != '':
    #     path='./metric_config/'+str(metric_id)+'.json'
    #     if os.path.exists(path):
    #         print(path)
    #         path = './metric_config/' + str(metric_id)+'_'+str(id) + '.json'
    #     with open(path, 'w') as f:
    #         json.dump(response, f)
    # print(response.text.encode('utf8'))
