import json
from glob import glob
import csv

result = {}
json_files = glob('./data/**/*.json', recursive=True)
print(json_files)


def flatten_json(key, value):
    if isinstance(value, dict):
        for sub_key, value in value.items():
            flatten_json(f'{key}_{sub_key}', value)
    elif isinstance(value, list):
        for i, item in enumerate(value):
            flatten_json(f"{key}_{i}", item)
    else:
        result[key] = value
    return result


def write_csv(dict_list, path):
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, dict_list[0].keys())
        writer.writeheader()
        writer.writerows(dict_list)


def main():
    # your code here

    dict_list = [
        flatten_json(key, value)
        for e in json_files
        for key, value in json.load(open(e)).items()
    ]
    if all(e.keys() == dict_list[0].keys() for e in dict_list):
        write_csv(dict_list,  "./output.csv")
    else:
        raise Exception("JSON files have different keys")


if __name__ == "__main__":
    main()
