import traceback
from types import GeneratorType
from typing import Any
import asyncio
from concurrent.futures import ThreadPoolExecutor

from quality_filter.iterator.base import JsonIterator, Message
from quality_filter.util.dicts import copy_val
from quality_filter.util.mod_util import load_cls


class If(JsonIterator):
    """流程选择节点，指定条件满足时执行"""
    def __init__(self, node: JsonIterator, matcher=None, key: str = None):
        assert node, "node is None"
        assert matcher or key, "matcher and key both None"
        if matcher is None:
            matcher = lambda r: r.get(key)
        elif isinstance(matcher, str):
            matcher = load_cls(matcher)[0]
        self.matcher = matcher
        self.node = node

    def __process__(self, data: Any, *args):
        if data is None:
            return None
        if isinstance(data, Message):
            if data.msg_type == 'end':
                res = self.node.__process__(data)
                if isinstance(res, GeneratorType):
                    for one in res:
                        if one is not None:
                            yield one
                return None
            data = data.data

        if not self.matcher(data):
            return data

        res = self.node.__process__(data)
        if isinstance(res, GeneratorType):
            for one in res:
                if one is not None:
                    yield one
        else:
            return res


class IfElse(JsonIterator):
    """流程选择节点，指定条件满足时执行node_a，否则执行node_b"""
    def __init__(self, node_a: JsonIterator, node_b: JsonIterator, matcher=None, key: str = None):
        assert node_a and node_b, "node_a or node_b is None"
        assert matcher or key, "matcher and key both None"
        if matcher is None:
            matcher = lambda r: r.get(key)
        elif isinstance(matcher, str):
            matcher = load_cls(matcher)[0]
        self.matcher = matcher
        self.node_a = node_a
        self.node_b = node_b

    def __process__(self, data: Any, *args):
        if data is None:
            return None
        if isinstance(data, Message):
            if data.msg_type == 'end':
                self.node_a.__process__(data)
                self.node_b.__process__(data)
                return None
            data = data.data
        if self.matcher(data):
            res = self.node_a.__process__(data)
        else:
            res = self.node_b.__process__(data)

        if isinstance(res, GeneratorType):
            for one in res:
                if one is not None:
                    yield one


class While(If):
    """循环节点，重复执行某个节点，直到条件不满足"""
    def __init__(self, node: JsonIterator, matcher=None, key: str = None, max_iterations: int = -1):
        super().__init__(node, matcher=matcher, key=key)
        self.max_iterations = max_iterations

    def __process__(self, data: Any, *args):
        if data is None:
            return None
        if isinstance(data, Message):
            if data.msg_type == 'end':
                res = self.node.__process__(data)
                if isinstance(res, GeneratorType):
                    for one in res:
                        if one is not None:
                            yield one
                    return
                else:
                    return res
            data = data.data

        ith = 0
        queue = [data]
        while queue:
            new_queue = []
            unfinished = False
            for one in queue:
                if one and self.matcher(one):
                    unfinished = True
                    res = self.node.__process__(one)
                    if isinstance(res, GeneratorType):
                        for one2 in res:
                            if one2 is not None:
                                new_queue.append(one2)
                    else:
                        new_queue.append(res)
            if not unfinished:
                break
            else:
                queue = new_queue
                ith += 1
                if 0 < self.max_iterations <= ith:
                    break

        for one in queue:
            yield one


class Multiple(JsonIterator):
    """多个节点组合"""
    def __init__(self, *nodes):
        """
        :param *nodes 处理算子
        """
        self.nodes = list(nodes)

    def add(self, iterator: JsonIterator):
        """添加节点"""
        self.nodes.append(iterator)
        return self

    def on_start(self):
        for it in self.nodes:
            it.on_start()

    def on_complete(self):
        for it in self.nodes[::-1]:
            it.on_complete()

    def __str__(self):
        nodes = [str(it) for it in self.nodes] 
        return f'{self.name}(nodes={nodes})'
        # return f'{self.name}()'


class Fork(Multiple):
    """
    分叉节点（并行逻辑），各处理节点独立运行。
    Fork节点本身不产生输出，因此不能与其他节点串联
    """
    def __init__(self, *nodes, copy_data: bool = False):
        """
        :param *nodes 处理算子
        :param copy_data 是否复制数据，使得各个分支对数据修改互不干扰
        """
        super().__init__(*nodes)
        self.copy_data = copy_data

    def __process__(self, data: Any, *args):
        for node in self.nodes:
            _data = data
            if self.copy_data:
                _data = copy_val(_data)
            res = node.__process__(_data, *args)
            # 注意：包含yield的函数调用仅返回迭代器，而不会执行函数
            if isinstance(res, GeneratorType):
                for _ in res:
                    pass


class Chain(Multiple):
    """
    链式组合节点（串行逻辑），前一个的输出作为后一个的输入。
    """
    def __init__(self, *nodes):
        super().__init__(*nodes)

    def walk(self, data: Any, break_when_empty: bool = True, end_msg: bool = False) -> list[Any]:
        """将前一个节点的输出作为下一个节点的输入，依次执行每个节点。返回最后一个节点的输出"""
        queue = [data]
        for node in self.nodes:
            new_queue = []  # cache for next processor, though there's only one item for most time
            # iterate over the current cache
            for current in queue:
                try:
                    res = node.__process__(current)
                except Exception as e:
                    print("ERROR! node: ", node, "data:", current)
                    traceback.print_exc()
                    raise e
                # 注意：包含yield的函数调用仅返回迭代器，而不会执行函数
                if isinstance(res, GeneratorType):
                    for one in res:
                        if one is not None:
                            new_queue.append(one)
                else:
                    if res is not None:
                        new_queue.append(res)

            # empty, check if break the chain
            if not new_queue and break_when_empty:
                return new_queue

            if end_msg:
                # send END msg in the end
                new_queue.append(None)

            queue = new_queue
        return queue

    def __process__(self, data: Any, *args):
        # print('Chain.__process__', data)
        # 普通流程中如果收到None 则中断执行链条
        if data is None:
            return None

        # 特殊消息处理
        if isinstance(data, Message):
            # 收到结束消息 后续节点还需要
            if data.msg_type == 'end':
                # print(f'{self.name}: END/Flush signal received.')
                queue = self.walk(data.data, break_when_empty=False, end_msg=True)
                if queue:
                    for one in queue:
                        yield one
                return
            else:
                data = data.data

        # 返回结果 从而支持与其他节点串联
        queue = self.walk(data)
        if queue:
            for one in queue:
                yield one


class Aggregate(JsonIterator):
    """
    聚合节点，将多个处理方法的结果汇总到一个数组中，保持结果顺序与节点添加顺序一致。
    每个处理方法可以是独立的节点或函数，它们会并行处理输入数据，
    最终将所有结果按节点添加顺序收集到一个列表中输出。
    """
    def __init__(self, *processors, copy_data: bool = False, max_workers: int = None):
        """
        :param processors: 处理方法或节点，可以是 JsonIterator 实例或可调用对象
        :param copy_data: 是否复制数据，使得各个处理方法对数据修改互不干扰
        :param max_workers: 并行处理的最大工作线程数
        """
        self.processors = list(processors)
        self.copy_data = copy_data
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)

    def add(self, processor):
        """添加处理方法或节点"""
        self.processors.append(processor)
        return self

    def on_start(self):
        """初始化所有子节点"""
        for processor in self.processors:
            if hasattr(processor, 'on_start'):
                processor.on_start()

    def on_complete(self):
        """完成所有子节点的处理"""
        for processor in reversed(self.processors):
            if hasattr(processor, 'on_complete'):
                processor.on_complete()
        self.executor.shutdown()

    async def _process_processor(self, index, processor, data, args):
        """异步处理单个处理器，并返回结果和索引"""
        try:
            _data = copy_val(data) if self.copy_data else data
            if hasattr(processor, '__process__'):
                # 处理 JsonIterator 类型的处理器
                res = processor.__process__(_data, *args)
                if isinstance(res, GeneratorType):
                    return index, list(res)
                else:
                    return index, [res] if res is not None else []
            elif callable(processor):
                # 处理普通可调用对象（函数）
                res = processor(_data)
                return index, [res] if res is not None else []
            else:
                return index, []
        except Exception as e:
            print(f"ERROR! processor: {processor}, data: {_data}")
            raise e

    def __process__(self, data: Any, *args):
        """处理数据并聚合所有结果到一个数组中，保持顺序"""
        if data is None:
            yield []
            return

        if isinstance(data, Message):
            if data.msg_type == 'end':
                # 处理结束消息，顺序执行所有处理器
                results = []
                for processor in self.processors:
                    if hasattr(processor, '__process__'):
                        res = processor.__process__(data)
                        if isinstance(res, GeneratorType):
                            results.extend(list(res))
                        elif res is not None:
                            results.append(res)
                yield results
                return
            data = data.data

        # 并行处理普通数据，收集所有处理器的结果
        loop = asyncio.get_event_loop()
        tasks = [
            self._process_processor(i, processor, data, args)
            for i, processor in enumerate(self.processors)
        ]
        
        # 执行所有任务并等待完成
        results = loop.run_until_complete(asyncio.gather(*tasks))
        
        # 按索引排序结果
        sorted_results = [res for _, res in sorted(results, key=lambda x: x[0])]
        
        # 展平结果列表
        flattened_results = []
        for sublist in sorted_results:
            flattened_results.extend(sublist)
        
        yield flattened_results


# class Aggregate(JsonIterator):
#     """
#     聚合节点，将多个处理方法的结果汇总到一个数组中。
#     每个处理方法可以是独立的节点或函数，它们会并行处理输入数据，
#     最终将所有结果收集到一个列表中输出。
#     """
#     def __init__(self, *processors, copy_data: bool = False):
#         """
#         :param processors: 处理方法或节点，可以是 JsonIterator 实例或可调用对象
#         :param copy_data: 是否复制数据，使得各个处理方法对数据修改互不干扰
#         """
#         self.processors = list(processors)
#         self.copy_data = copy_data

#     def add(self, processor):
#         """添加处理方法或节点"""
#         self.processors.append(processor)
#         return self

#     def on_start(self):
#         """初始化所有子节点"""
#         for processor in self.processors:
#             if hasattr(processor, 'on_start'):
#                 processor.on_start()

#     def on_complete(self):
#         """完成所有子节点的处理"""
#         for processor in reversed(self.processors):
#             if hasattr(processor, 'on_complete'):
#                 processor.on_complete()

#     def __process__(self, data: Any, *args):
#         """处理数据并聚合所有结果到一个数组中"""
#         if data is None:
#             yield []
#             return

#         if isinstance(data, Message):
#             if data.msg_type == 'end':
#                 # 处理结束消息，将消息传递给所有处理器
#                 results = []
#                 for processor in self.processors:
#                     if hasattr(processor, '__process__'):
#                         res = processor.__process__(data)
#                         if isinstance(res, GeneratorType):
#                             results.extend(list(res))
#                         elif res is not None:
#                             results.append(res)
#                 yield results
#                 return
#             data = data.data

#         # 处理普通数据，收集所有处理器的结果
#         results = []
#         for processor in self.processors:
#             _data = copy_val(data) if self.copy_data else data
#             try:
#                 if hasattr(processor, '__process__'):
#                     # 处理 JsonIterator 类型的处理器
#                     res = processor.__process__(_data, *args)
#                     if isinstance(res, GeneratorType):
#                         results.extend(list(res))
#                     elif res is not None:
#                         results.append(res)
#                 elif callable(processor):
#                     # 处理普通可调用对象（函数）
#                     res = processor(_data)
#                     if res is not None:
#                         results.append(res)
#             except Exception as e:
#                 print(f"ERROR! processor: {processor}, data: {_data}")
#                 traceback.print_exc()
#                 # 可选：可以选择忽略错误继续处理其他处理器，或根据需要调整
#                 raise e

#         # 生成聚合结果
#         yield results