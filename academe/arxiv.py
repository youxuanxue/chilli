# !/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright @2022 AI, ZHIHU Inc. (zhihu.com)
#
# @author: xuejiao <xuejiao@zhihu.com>
# @date: 2022/11/29
#
import json

import pandas
from oaipmh.client import Client
from oaipmh.metadata import MetadataRegistry, oai_dc_reader


def download():
    URL = "http://export.arxiv.org/oai2"
    registry = MetadataRegistry()
    registry.registerReader("oai_dc", oai_dc_reader)
    client = Client(URL, registry)

    list = client.listRecords(
        metadataPrefix='oai_dc',
        set='eess',  # http://export.arxiv.org/oai2?verb=ListSets
        # from_=datetime.datetime(2022, 11, 1, 1, 1, 1)
    )
    # count = 0
    with open("result_eess.txt", "w") as writer:
        for record in list:
            header, metadata, _ = record
            str = json.dumps(metadata._map)
            writer.write(str + "\n")
            # count += 1
            # print(str)
            # if count == 10:
            #     break
    print("done")


def flatten(values, sep):
    values = [v.strip().split(sep) for v in values]
    flat = [item.strip() for sublist in values for item in sublist]
    return flat


def filter_map(item_map, keep_keys):
    result = {}
    for key in keep_keys:
        if key == 'date':
            result[key] = item_map[key][0].split('-')[0]
        elif key == 'subject':
            # 对 subject 进行清理，把一些逗号连起来的也分割开
            result[key] = flatten(item_map[key], ',')
        elif key in ['title', 'identifier']:
            # 只保留 1 个 value
            result[key] = item_map[key][0]
        else:
            result[key] = item_map[key]
    return result


def statistic_subject():
    records = []
    columns = ['title', 'subject', 'date']
    with open('result_cs.txt', 'r') as reader:
        for line in reader.readlines():
            item_map = json.loads(line)
            current_item_map = filter_map(item_map, columns)
            if int(current_item_map['date']) < 2012:
                continue
            records.append(current_item_map)
            # break
    df = pandas.DataFrame.from_records(records, columns=columns)
    print(df.shape)
    df = df.drop_duplicates(subset=['title'])[['date', 'subject']]
    df = df.explode('subject')

    # df = df[df.subject.str.len() > 5]
    # 过滤一些不可读的 subject,e.g. 包括数字
    df = df[~df.subject.str.match('.*[0-9].*')]

    columns = ["date", "subject", "count"]
    df = pandas.DataFrame(df.value_counts()).reset_index()
    df.columns = columns
    df.set_index(columns)
    print(df.shape)
    print(df)
    print(type(df))
    print(df.columns)
    df.to_excel("statistic_subject.xlsx")


if __name__ == '__main__':
    statistic_subject()