## 处理节点（Iterator）

模块：`quality_filter.iterator`

组件构造：`Comp(*args, **kwargs)` `<module>.<Comp>(*args, **kwargs)`


### 基类设计
1. 抽象基类 `JsonIterator` 定义了数据处理的接口
```python
class JsonIterator:
    def on_start(self):
        pass

    def on_data(self, data: Any, *args):
        pass

    def __process__(self, data: Any or None):
        pass
    
    def on_complete(self):
        pass

    @property
    def name(self):
        return self.__class__.__name__
```

提供`_set`方法，支持链式设置组件属性，如`Count()._set(ticks=100)._set(label='aaa')`

### 组合节点
1. 并行处理 `Fork(*nodes)`
2. 串行处理 `Chain(*nodes)`
3. 重复数据 `Repeat(num_of_repeats)`
4. 结果聚合 `Aggregate(*nodes, copy_data=True, max_workers=2)` 

### 基础类
1. 打印数据 `Print` 方便调试或日志记录 无参数
2. 计数 `Count(ticks=1000, label='-')` 对数据进行统计，方便观察 参数：ticks、label

### 修改转换
1. 投影操作 `Select(*keys)` 支持嵌套字段 如`user.name`
2. 移除字段 `RemoveFields(*keys)`
3. 重命名字段 `RenameFields(**kwargs)`
4. 字段添加 `AddFields(**kwargs)` 仅添加不存在的字段
5. 字段填充 `InjectField(kv,inject_path, reference_path)`
6. 复制字段 `CopyFields(*keys)` 复制已有的字段 如果目标字段名存在 则覆盖
7. 拼接字段 `ConcatFields(target_key,*source_keys, sep='_')` 将source_keys拼接作为target_key字段
8. csv文件格式转换 `CSVToJSONConverter(csv_path, json_path)`


