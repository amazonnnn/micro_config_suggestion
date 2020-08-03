import json
import pandas as pd
import uuid


def json_load(path):
    # path = './metric_config_test/' + str(789) + '.json'
    with open(path, 'r') as f:
        load_dict = json.load(f)
    # load_dict = load_dict[0]
    return load_dict


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
# print(tag_dic)

config_store = {'wholeMetricConfig_Change_Smart': {'Change': {},
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
                'wholeMetricConfig_Smart': {},
                }

# for i in range(len(config_store)):
#     config_store[i] = '_'.join(config_store[i].split('_')[1:])

# print(config_store)
tag_id = 0
table_tag = []
table_store = []
table_suggest = []


def count_num(dic):
    sum = 0
    stop = False
    for key in dic:
        if type(dic[key]) == int:
            sum += dic[key]
            stop = True
        else:
            return count_num(dic[key])
    return sum


path = './metric_config_test/' + str('0801_2317') + '.json'
load_dict = json_load(path)

method_dic = ['Change_Smart', 'Change_Hard', 'Hard_Smart', 'Change_Hard_Smart', 'Change', 'Hard', 'Smart']
tag_result = []
uuid.uuid3(uuid.NAMESPACE_DNS, 'tag')
# build the storage_table
for i, tags in enumerate(load_dict):
    # print(i, tags)
    if i != 0:  # only use subtags from metrics
        break
    for tag in tags:
        # print(i,tag)
        # print(tags[tag])
        config_dic = {}
        # print(tag)
        # print(tags[tag])
        data = tags[tag]
        # print(data)
        current = []
        tag_uuid = uuid.uuid3(uuid.NAMESPACE_DNS, tag)
        current.append(str(tag_uuid))
        tag_id += 1
        table_tag.append([tag, tag_uuid, tag_dic[tag]])
        # current += [tags[tag][item] for item in method_dic]
        # print(current)
        for key in tags[tag]:
            # tem = [tag_uuid, ]
            # print(tags[tag][key])
            tem = [str(tag_uuid), key]
            # for item in tags[tag][key]:
            #     if type(tags[tag][key][item])!=dict:
            #         if item in method_dic:
            for item in method_dic:
                # print(item)
                tem.append(tags[tag][key][item])
            # print('start')
            for item in tags[tag][key]:
                # print(item)
                if type(tags[tag][key][item]) == dict:
                    # tem.append(item)
                    # print(item)
                    # print(111,json.dumps(tags[tag][key][item]))
                    tem.append(json.dumps(tags[tag][key][item]))
            # print(tem)
            tag_result.append(tem)

#
tag_result = pd.DataFrame(tag_result)
path = './metric_config_test/' + str('table1_0801_2317') + '.csv'
head = ['Tag_uuid', 'Granularity', 'Change_Smart', 'Change_Hard', 'Hard_Smart',
        'Change_Hard_Smart', 'Change', 'Hard', 'Smart', 'Change_Smart_config', 'Change_Hard_config',
        'Hard_Smart_config',
        'Change_Hard_Smart_config', 'Change_config', 'Hard_config', 'Smart_config']
tag_result.to_csv(path, index=False, header=head)

table_tag = pd.DataFrame(table_tag)
path = './metric_config_test/' + str('table2_0801_2317') + '.csv'
head = ['Tag_name', 'Tag_uuid', 'Category']
table_tag.to_csv(path, index=False, header=head)
