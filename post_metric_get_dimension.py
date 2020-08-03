import requests
import json
import os
import pandas as pd
from boundaryHelper import transformSensitivity

def get_csv_pd(path):
    dd = pd.read_csv(path)
    data1 = dd.iloc[:]
    # 如果全是数字用dd.loc[:,[4,5]]
    # .values.tolist()可以将数据转为list
    return data1.values.tolist()


top_num = 5
metrics = get_csv_pd('metric_config_test/metric.csv')
# print(metrics)
url_data = "https://kensho2-ppe-frontdoor.azurewebsites.net/metrics/series/data"
count=0
passed=0
for root, dirs, files in os.walk('./metric_config_test/series_0801/'):
    pass
print(files)

for metric in metrics:
    # print(metric)
    url = "https://kensho2-ppe-frontdoor.azurewebsites.net/metrics/" + metric[0] + "/rank-series"
    if metric[0]+'.json' in files:
        continue
    time = metric[2]
    json_file = {'startTime': time,
                 'dimensions': {},
                 'count': top_num}
    headers = {
        'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6Imh1Tjk1SXZQZmVocTM0R3pCRFoxR1hHaXJuTSIsImtpZCI6Imh1Tjk1SXZQZmVocTM0R3pCRFoxR1hHaXJuTSJ9.eyJhdWQiOiI4ZTBjMzYyOS05ODE5LTRjYWQtOTQ3MS04ZTZjZjY5OWViMTciLCJpc3MiOiJodHRwczovL3N0cy53aW5kb3dzLm5ldC83MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDcvIiwiaWF0IjoxNTk2Mjg1MTQ1LCJuYmYiOjE1OTYyODUxNDUsImV4cCI6MTU5NjI4OTA0NSwiYWlvIjoiQVdRQW0vOFFBQUFBRTJHQUxWOFd6TUxJalZ1Y1NYTFR0TzBHNkdZb0UyT3pxdGFaclVlOG53YkJPNDJsdUtoa0xIQnlaV1lOUmtYeFNvTXE2TklzSEppMEJnaXZCb0JxdXVtV3ZHQ2dSUERySkxtUGsrK01Kb1NIVHdtbE1mMjNqWlFoaUN0VERVQ3QiLCJhbXIiOlsicnNhIiwibWZhIl0sImZhbWlseV9uYW1lIjoiWmhhbyIsImdpdmVuX25hbWUiOiJIYW5nIiwiaXBhZGRyIjoiMTY3LjIyMC4yMzIuNTEiLCJuYW1lIjoiSGFuZyBaaGFvIiwibm9uY2UiOiI5ZGRjYTIyYS01OGI3LTQwNDMtYWI5MC1jNzM0Yzg3M2Y5NWEiLCJvaWQiOiI1YzY5NTdhMS0zMzNhLTRlNTEtOGE4Zi01Yzg2ZmMzN2EzZGQiLCJvbnByZW1fc2lkIjoiUy0xLTUtMjEtMjE0Njc3MzA4NS05MDMzNjMyODUtNzE5MzQ0NzA3LTI2NDc1NzgiLCJyaCI6IjAuQVJvQXY0ajVjdkdHcjBHUnF5MTgwQkhiUnlrMkRJNFptSzFNbEhHT2JQYVo2eGNhQUMwLiIsInN1YiI6Ikd1SVdMWnFUNmZrZUZSTUwwSk1hcjN6WE5iVDNHcGkxbHRoVDZNd0FlSmsiLCJ0aWQiOiI3MmY5ODhiZi04NmYxLTQxYWYtOTFhYi0yZDdjZDAxMWRiNDciLCJ1bmlxdWVfbmFtZSI6Imhhbmd6aGFvQG1pY3Jvc29mdC5jb20iLCJ1cG4iOiJoYW5nemhhb0BtaWNyb3NvZnQuY29tIiwidXRpIjoiVmhhOFJnRUNpVXljTnkyM0VXZ0ZBQSIsInZlciI6IjEuMCJ9.rTSdhGyaxIo4xhH1bcb9YstivVROBhqTUIoSj8-n032A8BpxsulBnu312RqPfxXTKAUcESltFATR0t9mSGigmkyzqT9oWJYCj-zioraCF6ALQQS02i6O6U2Ps2LTET9L2xcavodscn2yTuBu7JHw_-vzT5qU9aUQJuAgdxyO8Jzs680j9yaNRUZX40-lZ8zaq0x3BOYiCXMzQCcYcDI0fG2KUTGmypk4d13GmyVc3FhNtfcZRg3DINkmaQtinvkOBUa9zTQzGmNGVi5Z3O4NHG7kvP8uFv8JnUCKgKCROwx9Uxhb9ITz2aKAJ6aJhbhLLP1egA-f4VderqBk_WR0zA',
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, json=json_file)
    response = response.json()  # .encode('utf8')
    # print(response)
    if 'value' not in response:
        passed+=1
        print('failed:',response)
        continue
    for item in range(len(response['value'])):
        response['value'][item]['dimension'] = response['value'][item].pop('dimensions')
        response['value'][item]['startTime'] = time
        response['value'][item]["endTime"] = "2020-08-01T00:00:00Z"
    count+=1
    response = requests.request("POST", url_data, headers=headers, json=response)
    response = response.json()  # .encode('utf8')
    path='metric_config_test/series_0801/'+metric[0]+'.json'
    with open(path, 'w') as f:
        json.dump(response, f)
print('total metric num:',count)


# print(response)
# path = './metric_config/' + str(123123123) + '.json'
# with open(path, 'w') as f:
#     json.dump(response, f)
# if response != '':
#     path='./metric_config/'+str(metric_id)+'.json'
#     if os.path.exists(path):
#         print(path)
#         path = './metric_config/' + str(metric_id)+'_'+str(id) + '.json'
#     with open(path, 'w') as f:
#         json.dump(response, f)
# print(response.text.encode('utf8'))
