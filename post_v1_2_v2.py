import requests
import json
import os
import pandas as pd
import numpy as np
from boundaryHelper import transformSensitivity
from calculate_boundaryunit import getboundaryUnits

for root, dirs, files in os.walk('./metric_config_test/series_0801/'):
    pass
print(files)


def get_csv_pd(path):
    dd = pd.read_csv(path)
    data1 = dd.iloc[:]
    return data1.values.tolist()


def save_pd_csv(path, result):
    df = pd.DataFrame(result)
    head = ['Metric_id', 'sensitivity']  # alternative
    df.to_csv(path, index=False, header=head)


metric_info = get_csv_pd('./metric_config_test/metric.csv')
metric_dic = {}
for metric in metric_info:
    metric_dic[metric[0]] = metric[1]
print(metric_dic)
explore_range=300
max_length = 50
min_length = 5
result = []
count = 0
total = 0
for file in files:
    total += 1
    sensitivity = metric_dic[file.split('.')[0]]
    with open('./metric_config_test/series_0801/' + file, 'r') as f:
        load_dict = json.load(f)
    # load_dict = load_dict[0]
    load_dict = load_dict['value']
    sensitivity_v2 = []
    # print(load_dict)
    # print(file)
    for i in range(len(load_dict)):
        serie = load_dict[i]
        data = serie['values']
        if len(data) > explore_range:
            data = data[:explore_range]
        if len(data) < min_length:
            count += 1
            continue
        # print(i, data)
        values, expected_values, anomalies, boundaryUnits = [], [], [], []
        alert = 0
        for j in data:
            # print(1)
            values.append(j[1])
            expected_values.append(j[2])
            # if j[3]:
            #     print('amazing')
            anomalies.append(1 if j[3] else 0)
            if not j[22]:
                alert = 1
            boundaryUnits.append(j[22])
            # print(sensitivity)
            # print(values)
            # print(anomalies)
        if alert:
            start = -1
            end = -1
            # print('original', len(values))
            for i in range(len(values)):
                if start == -1 and values[i]:
                    start = i
                if start != -1 and not values[i]:
                    end = i
                if end - start > max_length or i == len(values) - 1:
                    end = start + max_length
            if start == -1 or end - start < min_length:
                count += 1
                continue

            # print('start end:', start, end)
            # if not values[0]:
            #     continue
            # for i in values:
            #     if not i:
            #         continue
            # print(start,end)
            # print(values)
            values = values[start:end]
            # print(len(values))
            anomalies = anomalies[start:end]
            expected_values = expected_values[start:end]
            boundaryUnits = boundaryUnits[start:end]
            # print(len(anomalies))
            # print(start, end, 'start to end')
            # print('values', values)
            # print(anomalies)
            # print(len(values))
            # print('len', len(values))
            boundaryUnits = getboundaryUnits(values, anomalies)
            # print(boundaryUnits,'bound')
        sensitivity_v2.append(transformSensitivity(values, expected_values, anomalies, boundaryUnits, sensitivity))
        # break
    # print(sensitivity_v2)
    if len(sensitivity_v2) == 0:
        count += 1
        continue
    print('sensitivity_v2', np.average(sensitivity_v2))
    metric_name = file.split('.')[0]
    result.append([file, np.average(sensitivity_v2)])
    # break
print(count, 'Failed metric number')
print(total)
path = 'metric_config_test/v1_to_v2_0801_' + str(max_length) + '.csv'
save_pd_csv(path, result)
