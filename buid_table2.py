import pandas as pd
import json


def get_csv_pd(path, id):
    dd = pd.read_csv(path)
    data1 = dd.iloc[:, id]
    return data1.values.tolist()


# table 1
path = './metric_config_test/' + str('table2_0801_2317') + '.csv'
data = get_csv_pd(path, [0, 1])
print(data)

with open("./metric.json", 'r', encoding='utf-8') as load_f:
    load_dict = json.load(load_f)

result = []
for i in load_dict[0]['rows']:
    if i[-2] != 'Active':
        continue
    # print('i',i)
    tem = [i[1],[]]
    for tag in data:
        # print('tag',tag)
        if tag[0] in i[-1].lower():
            tem[1].append(tag[1])
        # else:
        #     tem[1].append('')
        # print(tag)
    result.append(tem)
print(result)
for i in range(len(result)):
    # print(result[i][1])
    result[i][1]=' '.join(result[i][1])
df = pd.DataFrame(result)
path = './metric_config_test/' + str('table3_0801_2317') + '.csv'
head = ['Metric_id', 'Tag_id']
df.to_csv(path, index=False, header=head)
