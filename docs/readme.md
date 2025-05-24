# 通用处理器基类规范

## 设计原则
1. **强制生命周期**：所有子类必须实现`on_start`/`__process__`/`on_complete`
2. **明确职责分离**：预处理、核心处理、后处理阶段严格分离
3. **元信息标准化**：通过`name`和`__str__`提供统一标识

## 基础模板（Python）

```python
from typing import Any
from abc import ABC

class BaseProcessor(ABC):
    """所有处理器的抽象基类"""
    
    def __init__(self):
        """
        :param config: 处理器配置字典（可选）
        """
        pass
    
    # === 强制生命周期方法 ===
    def on_start(self) -> None:
        """预处理阶段（资源初始化/状态检查）"""
        pass

    def __process__(self, input_data: Any) -> Any:
        """核心处理逻辑（子类必须实现）
        :param input_data: 输入数据
        :return: 处理结果
        """
        raise NotImplementedError

    def on_complete(self) -> None:
        """后处理阶段（资源释放/结果持久化）"""
        pass
    
    # === 标准元信息 ===
    @property
    def name(self) -> str:
        """处理器名称（默认类名）"""
        return self.__class__.__name__

    def __str__(self) -> str:
        """字符串表示（日志/调试用）"""
        return f"{self.name}"
```

## 导出处理器方法

在quality_filter.iterator的__init__.py中使用相对导入方法，导入新增处理器方法
例如 `from .score import Comprehensive`


## yaml文件定义

### 常用字段
1. `name: str` 【必需】流程名称
2. `version: str` 流程版本号
3. `author: str` 作者
4. `description: str` 流程描述
5. `nodes: dict` 处理节点组件（包括动态变量定义 后定义的变量可引用前面定义的变量） 支持python表达式
6. `loader: str` 【必须】数据加载器组件，可引用`nodes`中已定义节点或创建新的节点
7. `processor: str` 【必须】数据处理器组件，通过引用`nodes`中变量定义主流程
8. `from: str or list` 集成的其他流程定义，支持单个文件或一组文件，如果文件不存在或出现循环引用将报错

总的来说，本框架实现的就是从`loader`加载数据 并通过`processor`进行处理

### nodes
1. 串行处理 `Chain(*nodes)` 链式组合节点（串行逻辑），前一个的输出作为后一个的输入。
2. 结果聚合 `Aggregate(*nodes, copy_data=True, max_workers=2)` ，并行处理，将多个处理方法的结果汇总到一个数组中。
3. 打印数据 `Print` 方便调试或日志记录 无参数
4. csv文件格式转换 `CSVToJSONConverter(csv_path, json_path)`

### loader
功能：定义流程的数据源节点（目前仅支持单个数据源节点）。节点定义可引用nodes节点。
1. 按行读取文本文件 `Text(input_file, encoding="utf8")` 每行为字符串直接传递。
2. JSON行文件 `JsonLine(input_file, encoding="utf8")` 每行按照JSON进行解析并传递。
3. JSON数组文件 `JsonArray(input_file, encoding="utf8")` 整个文件为一个JSON数组，依次传递数组中的每个元素。
4. JSON文件 `Json(input_file, encoding="utf8")` 整个文件为一个JSON对象传递给后续节点。
5. JSON自由文件 `JsonFree(input_file, encoding="utf8")` 针对格式化json文件，自动检测JSON对象并传递给后续节点。
6. CSV文件 `CSV(input_file, sep: str = ',', with_header: bool = False, encoding='utf8')` 按照CSV文件进行解析，如果带有表头，则以字典结构进行传递，否则以单元格列表进行传递。
7. YAML文件 `Yaml(input_file, encoding="utf8")` 加载yaml文件，作为一个对象传递。
8. 纯文本文件 `TextPlain(input_file: str, encoding: str = "utf8", **kwargs)` 加载文本文件，作为一个字符串传递

### processor
功能：定义流程的处理节点。节点定义可引用nodes节点。由于大部分数据处理为链式处理，因此经常用Chain进行流程组装。


### from
功能：指定当前流程继承的流程，其值为一个或多个（数组）流程文件路径。
详细说明见`yaml-flow.md`文件


## 启动
python main.py flow/qa_test.yaml 
