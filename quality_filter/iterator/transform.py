import csv
import json
from typing import Any
from quality_filter.iterator.base import JsonIterator


def detect_encoding(file_path):
    """ 尝试常见编码格式检测 """
    encodings = ['utf-8', 'gbk', 'gb18030', 'big5', 'iso-8859-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                f.read(1024)  # 预读部分内容测试
                return encoding
        except UnicodeDecodeError:
            continue
    return 'utf-8'  # 默认回退


class CSVToJSONConverter(JsonIterator):
    def __init__(self, csv_file_path: str, json_file_path: str):
        self.csv_file_path = csv_file_path
        self.json_file_path = json_file_path

    def on_data(self, data: Any):
        """处理数据的方法。"""
        file_encoding = detect_encoding(self.csv_file_path)

        data = []
        try:
            with open(self.csv_file_path, 'r', encoding=file_encoding, errors='replace') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                for row in csv_reader:
                    processed_row = {}
                    for key, value in row.items():
                        # 增强的异常处理
                        try:
                            processed_row[key] = float(value) if '.' in value else int(value)
                        except:
                            processed_row[key] = value.strip()
                    data.append(processed_row)
        except Exception as e:
            print(f"解码失败，最后尝试用 latin1 编码解析（可能丢失非ASCII字符）")
            with open(self.csv_file_path, 'r', encoding='latin1') as csv_file:
                csv_reader = csv.DictReader(csv_file)
                data = [row for row in csv_reader]

        with open(self.json_file_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=2, ensure_ascii=False)
        return data


# 使用示例
# if __name__ == "__main__":
#     csv_to_json('data.csv', 'data.json')
#     print("转换完成，请检查output.json文件内容是否完整")