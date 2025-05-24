from typing import Any

class score:
    """
    评分模块
    """

    def __init__(self):
        """
        初始化评分模块
        """
        pass

    def on_start(self):
        """处理数据前，主要用于一些数据处理的准备工作，不应该用于具体数据处理"""
        pass

    def __process__(self):
        pass

    def on_complete(self):
        """结束处理。主要用于数据处理结束后的清理工作，不应该用于具体数据处理"""
        pass
    
    @property
    def name(self):
        return self.__class__.__name__

    def __str__(self):
        return f"{self.name}"


class Comprehensive(score):
    def __init__(self):
        """
        初始化评分模块
        """
        pass

    def __process__(self, input_data: Any):
        """
        计算综合得分
        :return: 综合得分
        """
        if not input_data:
            return
        self.metrics_dict = {
            "SpecialCharacter": input_data[0].value,
            "ending": input_data[1].value
        }
        self.weights_dict = {
            "SpecialCharacter": 0.6,
            "ending": 0.4
        }
        self.strategy = "weighted_sum"
        # 输入校验
        assert set(self.metrics_dict.keys()) == set(self.weights_dict.keys()), "指标与权重不匹配"
        assert abs(sum(self.weights_dict.values()) - 1.0) < 1e-6, "权重和不等于1"

        # 数值标准化（示例：延迟类指标取倒数）
        normalized = {}
        for k, v in self.metrics_dict.items():
            if k == 'latency':  # 延迟类指标逆向处理
                normalized[k] = 1 / (1 + v / 1000)
            else:
                normalized[k] = max(0, min(v, 1))  # 常规指标限制在[0,1]

        # 合并策略选择
        if self.strategy == 'weighted_sum':
            res = sum(normalized[k] * self.weights_dict[k] for k in self.metrics_dict)
        elif self.strategy == 'product':
            res = 1.0
            for k in self.metrics_dict:
                res *= (normalized[k] ** self.weights_dict[k])
        elif self.strategy == 'min_max':
            res = 0.5 * (min(normalized.values()) + max(normalized.values()))

        # 结果后处理
        return max(0, min(res * 100, 100))  # 输出百分制得分
