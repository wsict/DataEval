## 数据加载器（Loader）

模块：`qualiter_filter.loader`

构造器：`<Comp>(*args, **kwargs)` 或 `<module>.<Comp>(*args, **kwargs)`

### 基类设计
1. 抽象基类 `DataLoader` 定义了数据加载器的接口
2. 文件基类 `file.File` 文件数据加载器
3. 二进制文件基类 `file.BinaryFile`
4. 文本文件基类`text.TextBase`

### 文件加载器
1. 按行读取文本文件 `Text(input_file, encoding="utf8")` 每行为字符串直接传递。
2. JSON行文件 `JsonLine(input_file, encoding="utf8")` 每行按照JSON进行解析并传递。
3. JSON数组文件 `JsonArray(input_file, encoding="utf8")` 整个文件为一个JSON数组，依次传递数组中的每个元素。
4. JSON文件 `Json(input_file, encoding="utf8")` 整个文件为一个JSON对象传递给后续节点。
5. JSON自由文件 `JsonFree(input_file, encoding="utf8")` 针对格式化json文件，自动检测JSON对象并传递给后续节点。
6. CSV文件 `CSV(input_file, sep: str = ',', with_header: bool = False, encoding='utf8')` 按照CSV文件进行解析，如果带有表头，则以字典结构进行传递，否则以单元格列表进行传递。
7. YAML文件 `Yaml(input_file, encoding="utf8")` 加载yaml文件，作为一个对象传递。
8. 纯文本文件 `TextPlain(input_file: str, encoding: str = "utf8", **kwargs)` 加载文本文件，作为一个字符串传递

### 文件夹加载器
通用文件夹加载 `Directory(folders, *suffix, recursive=False, type_mapping={}) `，参数说明：
- folders 指定文件或文件夹 
- *suffix 指定后缀名数组 如'.json' '.csv'，'all'表示全部支持的类型（此时其他参数会被忽略）
- recursive 进行递归处理，如果为True，会遍历子文件夹
- type_mapping 对文件类型进行映射 如`{'.json': '.jsonl'}`表示将`.json`文件当做`.jsonl`文件处理

已支持的文件类型（默认后缀名）：
- .txt -> Text
- .csv -> CSV
- .json -> Json
- .jsona -> JsonArray
- .jsonl -> JsonLine
- .jsonf -> JsonFree

### 其他加载器
1. 定时轮询加载器`TimedLoader` 可基于一个已有的加载器进行定时轮询 适合数据库轮询、服务监控等场景
2. 随机数生成器 `Random(num_of_times: int = 0)` 产生随机数（0~1）
3. 数组加载器 `Array(data: list)`
4. 字符串加载器 `String(text: str, sep: str = '\n')`
5. 函数加载器 `Function(function, *args, **kwargs)`
