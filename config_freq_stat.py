# -*- coding: utf-8 -*-
# 分tag打出各个group的df和met
import json
import re
import pandas as pd
import glob
import os
from collections import Counter


def get_csv_pd(path, id=None):
    dd = pd.read_csv(path)
    head = list(dd.keys())
    if id:
        data1 = dd.iloc[:, id]
    else:
        data1 = dd.iloc[:]
    return data1.values.tolist(), head


def history_detector(load_dict):
    # print('dada',load_dict)
    already = []
    if 'wholeMetricConfig' in load_dict:
        for key in load_dict['wholeMetricConfig']:
            if 'smart' in key.lower():
                already.append('Smart')
            if 'change' in key.lower():
                already.append('Change')
            if 'hard' in key.lower():
                # print(123)
                already.append('Hard')
    # else:
    #     raise ('Warning wholeMetricConfig not exists')
    already.sort()
    # if len(already)>1:
    #     print('??',already)
    return already


def dic_trans(dic):
    for key in dic:
        if type(dic[key]) != dict:
            dic[key] = {dic[key]: 1}.copy()
        else:
            dic[key] = dic_trans(dic[key]).copy()
    return dic.copy()


def shallow_copy(dic, load_dict):
    if type(load_dict) != dict:
        return
    for i in load_dict:
        if type(load_dict[i]) == dict:
            dic[i] = load_dict[i].copy()
        else:
            dic[i] = load_dict[i]
    for i in dic:
        shallow_copy(dic[i], load_dict[i])


def merge_dic(dic1, dic2):
    # print(dic1)
    # print(dic2)
    if type(dic2) != dict:
        return
    for key in dic2:
        if key not in dic1:
            dic1[key] = dic2[key]
        else:
            if type(dic1[key]) == int:
                # print(dic1,dic2)
                dic1[key] += dic2[key]
            else:
                merge_dic(dic1[key], dic2[key])
    # print(dic1)


def modify_dict(tags, already, tag, tag_id, load_dict, multi, gra, config_dic, version_dict, metric_name):
    already.sort()
    dic = {key: {} if key != 'Smart' else {'V1': {}, 'V2': {}} for key in already}.copy()
    top_tag = 'wholeMetricConfig'
    for key in dic:
        for item in load_dict[top_tag]:
            if key.lower() in item.lower():
                if key == 'Smart' and 'boundaryVersion' in load_dict[top_tag][item] and load_dict[top_tag][item][
                    'boundaryVersion'] == 2:
                    shallow_copy(dic[key]['V2'], load_dict[top_tag][item])
                elif key == 'Smart':
                    tem_store = [i[0] for i in version_dict]
                    if metric_name + '.json' in tem_store:
                        # print(metric_name)
                        # print(version_dict[[i[0] for i in version_dict].index(metric_name + '.json')][1])
                        # print(version_dict[
                        #           version_dict[[i[0] for i in version_dict].index(metric_name + '.json')], 1])
                        load_dict[top_tag][item]['sensitivity'] = \
                        version_dict[[i[0] for i in version_dict].index(metric_name + '.json')][1]
                        if dic[key]['V2']!={}:
                            # print('find one!!', load_dict[top_tag])
                            merge_dic(dic[key]['V2'], load_dict[top_tag][item])
                        else:
                            shallow_copy(dic[key]['V2'], load_dict[top_tag][item])
                    else:
                        shallow_copy(dic[key]['V1'], load_dict[top_tag][item])
                else:
                    shallow_copy(dic[key], load_dict[top_tag][item])
    # print('output', dic)
    dic = dic_trans(dic)
    if multi:
        recent = {}
        shallow_copy(recent, config_dic[top_tag + '_' + '_'.join(already)])
        tags[tag_id][tag][gra]['_'.join(already)] += 1
        for a in already:
            if tags[tag_id][tag][gra][top_tag + '_' + '_'.join(already)][a] == config_dic:
                tags[tag_id][tag][gra][top_tag + '_' + '_'.join(already)][a] = dic[a]
            else:
                merge_dic(tags[tag_id][tag][gra][top_tag + '_' + '_'.join(already)][a], dic[a])
    else:
        for a in already:
            recent = {}
            shallow_copy(recent, config_dic[top_tag + '_' + a])
            tags[tag_id][tag][gra][a] += 1
            if tags[tag_id][tag][gra][top_tag + '_' + a] == config_dic:
                tags[tag_id][tag][gra][top_tag + '_' + a] = dic[a]
            else:
                merge_dic(tags[tag_id][tag][gra][top_tag + '_' + a], dic[a])
    return tags


rubbish_word = ['data', 'count', 'using', 'user', 'users', 'get', 'size', 'using', 'one', 'and', 'per', 'tab', 'common',
                'time', '']
tags0 = ['cpu', 'memory', 'network', ' io ', 'latency', 'service error', 'availability', 'sla', 'traffic', 'perf',
         'dev',
         'percentile', 'spam', 'outlookdesktop', 'outlook_desktop', 'excel', 'demand_metric', 'demandmetric',
         'demand metric', 'demand metrics', 'data quality', 'data_quality',
         'demand_metrics', 'demandmetrics', 'rawlog', 'row_log', 'odin ml', 'odin_ml', 'metric platform',
         'metric_platform', 'yammer', 'metricplatform', 'sml', 'heartbeat', 'heart_beat',
         'devops', 'totaldevices', 'usagerate', 'koality', 'lighthouse', ' css ',
         'devicecount', 'peakusage', 'latency', 'service error', 'service_error', 'availability', 'sla', 'traffic',
         'reliablity', 'crashrate', 'duration', 'crash',
         'failurerate', 'loadtimeavg', 'rpm', 'revenue', 'uu', 'churn rate', 'churn_rate', 'churnrate', 'reture_rate',
         'returnrate', 'return rate', 'ctr', 'click', 'qbr'
                                                      'mau', 'dau', 'runrate', 'runrate', 'rolling28', 'rolling 28',
         'dsq', 'qps', 'qor', 'browserminutes', 'rpm', 'dsqs', 'nps',
         'queryviewcount', 'ecpm', 'costs', 'tenantcount',
         'heath care', 'heath_care', 'heathcare', 'manufacture', 'telecom', 'smart building', 'smart_building',
         'transportation', 'logcount', 'rowcount', 'entitycount',
         'dailycount', 'totalrows', 'ingestiondelay', 'rowcnt']
tag_devops = ['cpu', 'memory', 'network', ' io ', 'latency', 'service error', 'availability', 'sla', 'traffic', 'perf',
              'percentile', 'heartbeat', 'heart_beat',
              'dev', 'devops', 'totaldevices', 'usagerate']
tag_hardwares = ['cpu', 'memory', 'network', ' io ', 'devicecount', 'peakusage']
tag_qos = ['latency', 'service error', 'service_error', 'availability', 'sla', 'traffic', 'reliablity', 'crashrate',
           'duration', 'failurerate', 'loadtimeavg', 'rpm', 'outlookdesktop', 'outlook_desktop', 'excel',
           'metric platform', 'metric_platform', 'yammer', 'metricplatform', 'crash']
tag_busin = ['revenue', 'uu', 'churn rate', 'churn_rate', 'churnrate', 'reture_rate', 'returnrate', 'return rate',
             'ctr', 'click', 'demand_metric', 'demandmetric', 'demand_metrics', 'demandmetrics', 'demand metric',
             'demand metrics',
             'mau', 'dau', 'runrate', 'runrate', 'rolling28', 'rolling 28', 'dsq', 'qps', 'qor', 'browserminutes',
             'rpm', 'dsqs',
             'nps', 'queryviewcount', 'ecpm', 'costs', 'tenantcount', 'lighthouse', ' css ', 'qbr']
tag_iot = ['heath care', 'heath_care', 'heathcare', 'manufacture', 'telecom', 'smart building', 'smart_building',
           'transportation']
tag_qod = ['logcount', 'rowcount', 'entitycount', 'dailycount', 'totalrows', 'ingestiondelay', 'rowcnt', 'rawlog',
           'row_log', 'odin ml', 'odin_ml', 'koality', 'sml', 'data quality', 'data_quality']
tag_scu = ['spam']
team = ['odin', 'lighthouse', 'odin ml', 'metric platform', 'metric_platform', 'yammer', 'metricplatform',
        'outlookdesktop', 'outlook_desktop', 'excel', 'dev']
tag_dic = {}
name_dic = {'Peter Jarvis': 'Quality_of_Data'}
for i in tag_devops:
    tag_dic[i] = 'Devops'
for i in tag_hardwares:
    tag_dic[i] = 'Hardwares'
for i in tag_qos:
    tag_dic[i] = 'Quality_of_System'
for i in tag_busin:
    tag_dic[i] = 'Business'
for i in tag_iot:
    tag_dic[i] = 'Internet_Of_Things'
for i in tag_qod:
    tag_dic[i] = 'Quality_of_Data'
for i in tag_scu:
    tag_dic[i] = 'Secure'
print(tag_dic)

with open("./datafeed_720.json", 'r', encoding='utf-8') as load_f:
    load_dict = json.load(load_f)
result_datafeed = {}
for i in load_dict[0]['rows']:
    if 'test' in i[1] or 'Test' in i[1] or 'TEST' in i[1]:
        continue
    if i[-1] == 'Active':
        # print(i)
        result_datafeed[i[0]] = i[1:-1]

version_trans_dic, _ = get_csv_pd('./metric_config_test/v1_to_v2_0801_50.csv')
# print('version', version_trans_dic)

print(len(result_datafeed[3910]), 'len datafeed dic')
num = 0
for i in result_datafeed:
    num += 1
print(num, 'total number')
print(result_datafeed[3910])
# for i in result_datafeed:
#     print(result_datafeed[i])
# datafeed_name granularity_name granularity_amount, creator, datafeed_uuid


with open("./metric.json", 'r', encoding='utf-8') as load_f:
    load_dict = json.load(load_f)
for i in load_dict[0]['rows']:
    if i[2] in result_datafeed:
        if i[-2] != 'Active':
            pass
        # print(i)
        if len(result_datafeed[i[2]]) == 5:
            result_datafeed[i[2]].append([i[-1]])
            result_datafeed[i[2]].append([i[1]])
        else:
            result_datafeed[i[2]][5].append(i[-1])
            result_datafeed[i[2]][6].append(i[1])

for i in result_datafeed:
    # print(result_datafeed[i])
    if len(result_datafeed[i]) != 7:
        # print(result_datafeed[i])
        result_datafeed[i].append([])
        result_datafeed[i].append([])

# for i in result_datafeed:
#     print(result_datafeed[i])
# datafeed_name granularity_name granularity_amount, creator, data_uuid, metric, metric_uuid

with open("./dimension_0719.json", 'r', encoding='utf-8') as load_f:
    load_dict = json.load(load_f)
store = []
for i in load_dict[0]['rows']:
    if i[2] in result_datafeed:
        if i[2] not in store:
            store.append(i[2])
        if len(result_datafeed[i[2]]) == 7:
            result_datafeed[i[2]].append([i[-1]])
        else:
            result_datafeed[i[2]][7].append(i[-1])
        # print(i[-1])
        # result_metric[i[0]]=i[1:-1]
        # break
print(len(store))
for i in result_datafeed:
    if len(result_datafeed[i]) == 7:
        result_datafeed[i].append([])
# for i in result_datafeed:
#     print(result_datafeed[i])
#     break
# datafeed_name granularity_name granularity_amount, creator, datafeed_uuid, metric, metric_uuid, dimension

dd = pd.read_csv('./group/tag/stat.csv')
sub_m = dd.iloc[:, [0]]['subtag_m']
tag_m = dd.iloc[:, [2]]['tag_m']
sub_df = dd.iloc[:, [4]]['subtag_df']
tag_df = dd.iloc[:, [6]]['tag_df']
tags = [sub_m, tag_m, sub_df, tag_df]
# print('tags', tags)

for i in range(len(tags)):
    tags[i] = list(tags[i])
    # print(tags[i])
    for j in range(len(tags[i])):
        if type(tags[i][j]) != str:
            tags[i] = tags[i][:j]
            break
    # print(tags[i])
for i in range(len(tags)):
    tags[i] = {
        j: {
        } for
        j in tags[i]}

config_dic = {
    'Metric_num': 0, 'WO_config': 0, 'Override_count': 0, 'Hard': 0, 'Smart': 0, 'Change': 0,
    'Hard_Smart': 0, 'Change_Hard': 0, 'Change_Smart': 0, 'Change_Hard_Smart': 0,
    # 'wholeMetricConfig_conditionOperator': {},

    'wholeMetricConfig_Change_Smart': {'Change': {},
                                       'Smart': {}},
    'wholeMetricConfig_Change_Hard': {'Change': {},
                                      'Hard': {}},
    'wholeMetricConfig_Hard_Smart': {'Hard': {},
                                     'Smart': {}},
    'wholeMetricConfig_Change_Hard_Smart': {'Change': {},
                                            'Smart': {},
                                            'Hard': {}},

    'wholeMetricConfig_Change': {},
    'wholeMetricConfig_Hard': {},
    'wholeMetricConfig_Smart': {'V1': {}, 'V2': {}}
}

print(tags)
# tags to store the final result of tag_dict_config_frequency

# datafeed_name granularity_name granularity_amount, creator, datafeed_uuid, metric, metric_uuid, dimension
for root, dirs, files in os.walk('./metric_config'):
    print('files:', files)
    # pass
# print(len(files))


# result_datafeed: DF & Metric raw info
# tags: final result
# load_dict: data in json from post request
#
n = 0

for i in result_datafeed:
    if len(result_datafeed[i][-2]) == 0:
        continue
    granularity = result_datafeed[i][1]
    for j in range(len(result_datafeed[i][-2]) - 1, -1, -1):  # in metric
        file = result_datafeed[i][-2][j] + '.json'
        if file in files and len(json.load(open("./metric_config/" + file, 'r', encoding='utf-8'))) != 0:
            with open("./metric_config/" + file, 'r', encoding='utf-8') as load_f:
                load_dict = json.load(load_f)
            # if len(load_dict) == 0:
            #     continue
            load_dict = load_dict[0]
            # print(load_dict)
            for tag_id in range(4):  # in m or d tag or subtag
                if tag_id == 2 or tag_id == 3:
                    if j != 0:
                        break
                index = -3 if tag_id < 2 else 0
                # aa the df_name or metric_name
                aa = result_datafeed[i][index][j] if tag_id < 2 else result_datafeed[i][index]
                for tag in tags[tag_id]:  # for each tag
                    # print(111)
                    already = []
                    if tag in aa.lower():
                        # if tag_id==2:
                        #     print(tag)
                        # print(tags[tag_id])
                        if granularity not in tags[tag_id][tag]:
                            tags[tag_id][tag][granularity] = {}
                            shallow_copy(tags[tag_id][tag][granularity], config_dic)
                        if granularity not in tags[tag_id + 1]:
                            tags[tag_id + 1][tag_dic[tag]][granularity] = {}
                            shallow_copy(tags[tag_id + 1][tag_dic[tag]][granularity], config_dic)
                        tags[tag_id][tag][granularity]['Metric_num'] += 1
                        if len(load_dict['dimensionGroupOverrideConfigs']) != 0:
                            # print(load_dict['dimensionGroupOverrideConfigs'])
                            tags[tag_id][tag][granularity]['Override_count'] += 1
                        if tag_id == 0 or tag_id == 2:
                            tags[tag_id + 1][tag_dic[tag]][granularity]['Metric_num'] += 1
                        # modify config num
                        if load_dict['wholeMetricConfig']['conditionOperator'] == 'AND':
                            already = history_detector(load_dict)
                            # already = '_'.join(already)
                            # if len(already.split('_'))>1:
                            #     print(already)
                            if len(already) > 1:
                                multi = True
                            else:
                                multi = False
                            tags = modify_dict(tags, already, tag, tag_id, load_dict, multi, granularity, config_dic,
                                               version_trans_dic, result_datafeed[i][-2][j])
                        else:
                            already = history_detector(load_dict)
                            tags = modify_dict(tags, already, tag, tag_id, load_dict, False, granularity, config_dic,
                                               version_trans_dic, result_datafeed[i][-2][j])
        else:
            for tag_id in range(4):
                if tag_id == 2 or tag_id == 3:
                    if j != 0:
                        break
                index = -3 if tag_id < 2 else 0
                aa = result_datafeed[i][index][j] if tag_id < 2 else result_datafeed[i][index]
                for tag in tags[tag_id]:
                    if tag in aa.lower():
                        if granularity not in tags[tag_id][tag]:
                            tags[tag_id][tag][granularity] = {}
                            shallow_copy(tags[tag_id][tag][granularity], config_dic)
                        if granularity not in tags[tag_id + 1][tag_dic[tag]]:
                            tags[tag_id + 1][tag_dic[tag]][granularity] = {}
                            shallow_copy(tags[tag_id + 1][tag_dic[tag]][granularity], config_dic)
                        # print(tag_id,tag)
                        tags[tag_id][tag][granularity]['WO_config'] += 1
                        tags[tag_id][tag][granularity]['Metric_num'] += 1
                        if tag_id == 0 or tag_id == 2:
                            tags[tag_id + 1][tag_dic[tag]][granularity]['Metric_num'] += 1
                            tags[tag_id + 1][tag_dic[tag]][granularity]['WO_config'] += 1

# print('final result')
# print(tags)

# print(type(tags))
# path = './metric_config_test/' + str('0801_2317') + '.json'
# with open(path, 'w') as f:
#     print(tags)
#     json.dump(tags, f)
