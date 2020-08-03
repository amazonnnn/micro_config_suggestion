import pandas as pd
import json
import numpy as np

def save_json(path, response):
    # 区别是是否json化
    # response = response.json()  # .encode('utf8')
    with open(path, 'w') as f:
        json.dump(response, f)

def get_csv_pd(path, id=None):
    dd = pd.read_csv(path)
    head = list(dd.keys())
    if id:
        data1 = dd.iloc[:, id]
    else:
        data1 = dd.iloc[:]
    return data1.values.tolist(), head


def find_the_best(dic):
    count = 0
    result = ''
    for key in dic:
        if type(dic[key]) != dict:
            if dic[key] > count:
                count = dic[key]
                result = key
        else:
            suggest = find_the_best(dic[key])
            if suggest:
                dic[key] = suggest
    if count > 0:
        return result
    else:
        return False


def find_most(dic):
    count = 0
    for key in dic:
        if dic[key] > count:
            result = key
    return result


def suggest_config(dic):
    if dic == {}:
        return {}
    if 'V2' in dic:
        if dic['V2'] != {}:
            dic.pop('V1')
            return suggest_config(dic['V2'])
        else:
            dic.pop('V2')
            return suggest_config(dic['V1'])
    for key in dic:
        if type(dic[key]) == int:
            return find_most(dic)
        else:
            dic[key] = suggest_config(dic[key])
    return dic


# table 1

path = './metric_config_test/' + str('table3_0801_2317') + '.csv'
datas, datas_head = get_csv_pd(path)
print(datas)

path = './metric_config_test/' + str('table1_0801_2317') + '.csv'
tags, tags_head = get_csv_pd(path)

path = './metric_config_test/v1_to_v2_0801_50.csv'
version_trans, version_trans_head = get_csv_pd(path)
trans_dic = {}
for i in version_trans:
    trans_dic[i[0]] = i[1]
print(trans_dic)
# print(tags)
# print(tags_head)
result = {}
for data in tags:
    if data[0] not in result:
        result[data[0]] = {}
for data in tags:
    result[data[0]][data[1]] = {'Suggest': '', 'Config': ''}
    suggest = tags_head[data.index(max(data[2:9]))]
    config = data[tags_head.index(suggest + '_config')]
    config = json.loads(config)
    result[data[0]][data[1]]['Suggest'] = suggest
    # metric_id = current[0]
    # print()
    # print(config)
    # print(suggest)
    # print(tags_head)
    # suggest_dic = {}
    # print()
    # print(suggest)
    # print(config)
    suggest_config(config)
    result[data[0]][data[1]]['Config'] = config
    print(result[data[0]])
    # print(json.dumps(config))
    print()
    # current.append(config.dumps())
path = './metric_config_test/suggest.json'
save_json(path,result)