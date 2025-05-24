import json
from typing import Dict, List, Optional
from pydantic import BaseModel
import re
from collections import defaultdict
import json
from collections import Counter
from typing import Callable, List, Set, Tuple
import string
import zhon.hanzi
import unicodedata
TRANSLATION_TABLE_PUNCTUATION_EN = str.maketrans('', '', string.punctuation)
TRANSLATION_TABLE_PUNCTUATION_ZH = str.maketrans('', '', zhon.hanzi.punctuation)

class DynamicRuleConfig(BaseModel):
    threshold: Optional[float] = None
    pattern: Optional[str] = None
    key_list: Optional[List[str]] = None
    refer_path: Optional[List[str]] = None
    
class ModelRes(BaseModel):
    error_status: bool = False
    type: str = 'QUALITY_GOOD'
    name: str = 'Data'
    value: Optional[float] = None
    reason: List[str] = []  
    
class BaseRule:
    # metric_type: str  # This will be set by the decorator
    # group: List[str]  # This will be set by the decorator
    # dynamic_config:  DynamicRuleConfig
    def __init__(self):
        pass

    def __process__(self, input_data) -> ModelRes:
        raise NotImplementedError()   
    
    def on_start(self):
        """处理数据前，主要用于一些数据处理的准备工作，不应该用于具体数据处理"""
        pass

    def on_complete(self):
        """结束处理。主要用于数据处理结束后的清理工作，不应该用于具体数据处理"""
        pass
    
    @property
    def name(self):
        return self.__class__.__name__

    def __str__(self):
        return f"{self.name}"
    
    
class Character(BaseRule):
    """check whether content has special characters. """
    def __init__(self):
        super().__init__()
        self.dynamic_config = DynamicRuleConfig(
            key_list=[
                r"u200e",
                # r"(\\\\;){3,}|(\{\}){3,}|(&nbsp;){3,}",
                r"&#247;|\? :",
                r"[�□]|\{\/U\}",
                r"U\+26[0-F][0-D]|U\+273[3-4]|U\+1F[3-6][0-4][0-F]|U\+1F6[8-F][0-F]",
                r"<\|.*?\|>"
            ]
        )


    def __process__(self, input_data) -> ModelRes:
        res = ModelRes()
        content = input_data[0]['data']
        if len(content) == 0:
            return res

        matches = []
        num = 0
        for p in self.dynamic_config.key_list:
            m = re.findall(p, content)
            num += len(m)
            matches = matches + m
        if num / len(content) >= 0.001:
            res.error_status = True
            res.value = num / len(content)
            #res.type = cls.metric_type
            res.name = self.__class__.__name__
            res.reason = list(set(matches))
        return res


class TextSlice:
    """A slice of text from a document."""

    def __init__(self, text: str, start: int, end: int):
        self.text = text
        self.start = start
        self.end = end

def normalize(
        text: str,
        remove_punct: bool = True,
        lowercase: bool = True,
        nfd_unicode: bool = True,
        white_space: bool = True
) -> str:
    """Normalize the text by lowercasing and removing punctuation."""
    # remove punctuation
    if remove_punct:
        text = text.translate(TRANSLATION_TABLE_PUNCTUATION_EN)
        text = text.translate(TRANSLATION_TABLE_PUNCTUATION_ZH)
    # lowercase
    if lowercase:
        text = text.lower()
    if white_space:
        text = text.strip()
        text = re.sub(r'\s+', ' ', text)
    # NFD unicode normalization
    if nfd_unicode:
        text = unicodedata.normalize('NFD', text)
    return text

def split_paragraphs(
            text: str, normalizer: Callable[[str], str], remove_empty: bool = True
    ) -> Tuple[TextSlice]:
        """
        Split a string into paragraphs. A paragraph is defined as a sequence of zero or more characters, followed
        by a newline character, or a sequence of one or more characters, followed by the end of the string.
        """
        text_slices = tuple(
            TextSlice(normalizer(text[match.start():match.end()]), match.start(), match.end())
            for match in re.finditer(r"([^\n]*\n|[^\n]+$)", text)
        )

        if remove_empty is True:
            text_slices = tuple(
                text_slice for text_slice in text_slices if text_slice.text.strip()
            )

        return text_slices

class EndWithTerminal(BaseRule):
    """check whether the ratio of line ends with terminal punctuation mark > 0.6 """

    def __init__(self):
        super().__init__()
        self.dynamic_config = DynamicRuleConfig(threshold=0.6, key_list=[".", "!", "?", "”", "\""])

    def __process__(self, input_data) -> ModelRes:
        res = ModelRes()
        raw_content = input_data[0]['data']
        raw_lines: Tuple[TextSlice] = split_paragraphs(
            text=raw_content, normalizer=lambda x: x, remove_empty=True
        )
        num_lines = len(raw_lines)
        if num_lines == 0:
            return res

        terminal_marks = [line.text.rstrip()[-1] for line in raw_lines if line.text and line.text.rstrip()[-1] not in self.dynamic_config.key_list]
        num_occurrences = sum([line.text.rstrip().endswith(tuple(self.dynamic_config.key_list)) for line in raw_lines])
        ratio = num_occurrences / num_lines
        res.value = ratio
        if ratio < self.dynamic_config.threshold:
            res.error_status = True
            #res.type = cls.metric_type
            res.name = self.__class__.__name__
            res.reason = list(set(terminal_marks))
        return res

class EndWithEllipsis(BaseRule):
    """check whether the ratio of line ends with ellipsis < 0.3 """

    def __init__(self):
        super().__init__()
        self.dynamic_config = DynamicRuleConfig(threshold=0.3, key_list=["...", "…"])
    def __process__(self,input_data) -> ModelRes:
        res = ModelRes()
        raw_content = input_data[0]['data']
        raw_lines: Tuple[TextSlice] = split_paragraphs(
            text=raw_content, normalizer=lambda x: x, remove_empty=True
        )
        num_lines = len(raw_lines)
        if num_lines == 0:
            return res

        num_occurrences = sum(
            [line.text.rstrip().endswith(tuple(self.dynamic_config.key_list)) for line in raw_lines])
        ratio = num_occurrences / num_lines
        res.value=ratio
        if ratio > self.dynamic_config.threshold:
            res.error_status = True
            #res.type = cls.metric_type
            res.name = self.__class__.__name__
            res.reason = ["The ratio of lines end with ellipsis is: " + str(ratio)]
        return res

class SentenceNumber(BaseRule):
    """check whether the number of sentence in [3, 7500] """

    def __init__(self):
        super().__init__()
        self.dynamic_config = DynamicRuleConfig(key_list=['3', '7500'])
        self.SENT_PATTERN = re.compile(r'\b[^.!?\n]+[.!?]*', flags=re.UNICODE)

    def __process__(self, input_data) -> ModelRes:
        res = ModelRes()
        raw_content = input_data[0]['data']
        num_sentence = len(self.SENT_PATTERN.findall(raw_content))
        res.value = num_sentence
        if num_sentence < int(self.dynamic_config.key_list[0]) or num_sentence > int(self.dynamic_config.key_list[1]):
            res.error_status = True
            #res.type = cls.metric_type
            res.name = self.__class__.__name__
            res.reason = ["The number of sentence is: " + str(num_sentence)]
        return res

class WordNumber(BaseRule):
    """check whether the number of word in [20, 100000] """
    def __init__(self):
        super.__init__()
        self.dynamic_config = DynamicRuleConfig(key_list=['20', '100000'])

    def __process__(self, input_data) -> ModelRes:
        res = ModelRes()
        normalized_content = normalize(input_data[0]['data'])
        normalized_words = tuple(normalized_content.split())
        num_normalized_words = len(normalized_words)
        res.value = num_normalized_words
        if num_normalized_words >= int(self.dynamic_config.key_list[0]) and num_normalized_words < int(self.dynamic_config.key_list[1]):
            pass
        else:
            res.error_status = True
            #res.type = cls.metric_type
            res.name = self.__class__.__name__
            res.reason = ["The number of word is: " + str(num_normalized_words)]
        return res

class ValidateFormat(BaseRule):
    def __init__(self,pattern):
        super().__init__()
        self.pattern = pattern
    def __process__(self,data) -> Dict[str, ModelRes]:
        """
        通用格式校验函数，根据传入正则表达式校验数据格式有效性
        忽略 None 和空字符串。
        返回：无效行数，有效行数，总行数，无效比率
        """
        regex = re.compile(self.pattern)
        valid_count = 0
        invalid_count = 0
        filtered_data = [x for x in data if x is not None and x.strip() != ""]

        for item in filtered_data:
            if regex.fullmatch(item.strip()):
                valid_count += 1
            else:
                invalid_count += 1

        total = len(filtered_data)
        invalid_ratio = invalid_count / total if total > 0 else 0.0

        invalid_count_ = ModelRes()
        invalid_count_.value = invalid_count
        valid_count_=ModelRes()
        valid_count_.value = valid_count
        total_ = ModelRes()
        total_.value = total
        invalid_ratio_ = ModelRes()
        invalid_ratio_.value = round(invalid_ratio, 4)
        return {
            "invalid_count": invalid_count_,
            "valid_count": valid_count_,
            "total": total_,
            "invalid_ratio": invalid_ratio_
        }


class ValidateEmail(ValidateFormat):
    def __init__(self):
        self.pattern= r'^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+(.[a-zA-Z0-9_-]+)+$'
        super().__init__(self.pattern)

class ValidateIDCard(ValidateFormat):
    def __init__(self):
        self.pattern=r'(^[1-9]\d{5}(18|19|20)\d{2}'
        r'((0[1-9])|(10|11|12))'
        r'(([0-2][1-9])|10|20|30|31)\d{3}[0-9Xx]$)'
        r'|'
        r'(^[1-9]\d{5}\d{2}'
        r'((0[1-9])|(10|11|12))'
        r'(([0-2][1-9])|10|20|30|31)\d{3}$)'
        super().__init__(self.pattern)

class ValidateIPAddress(ValidateFormat):
    def __init__(self):
        self.pattern=(r'^(?:(?:1[0-9][0-9].)|(?:2[0-4][0-9].)|(?:25[0-5].)|'
               r'(?:[1-9][0-9].)|(?:[0-9].)){3}'
               r'(?:(?:1[0-9][0-9])|(?:2[0-4][0-9])|(?:25[0-5])|'
               r'(?:[1-9][0-9])|(?:[0-9]))$')
        super().__init__(self.pattern)

class ValidatePhone(ValidateFormat):
    def __init__(self):
        self.pattern=r'^1[3-9]\d{9}$'
        super().__init__(self.pattern)

class ValidatePostcode(ValidateFormat):
    def __init__(self):
        self.pattern= r'^[1-9]\d{5}$'
        super().__init__(self.pattern)

class ValidateDate(ValidateFormat):
    def __init__(self):
        self.pattern=r'^[1-9]\d{3}-(0[1-9]|1[0-2])-(0[1-9]|[1-2][0-9]|3[0-1])$'
        super().__init__(self.pattern)


class CheckNullValues(BaseRule):
    def __init__(self):
        super().__init__()
    def __process__(self, input_data) -> Dict[str, ModelRes]:
    #check_null_values(data: Union[Dict, List], path: List[str]) -> Dict[Any, Any]:
        """
        检查空值：空字符串或None视为空值。
        返回：空值行数，总行数，空值率
        """
        data=input_data[0]['data']
        total = len(data)
        null_count = sum(1 for x in data if x is None or x.strip() == "")
        null_ratio = null_count / total if total > 0 else 0.0
        #return null_count, total, round(null_ratio, 4)
        null_count_ = ModelRes()
        null_count_.value = null_count
        total_ = ModelRes()
        total_.value = total
        null_ratio_ = ModelRes()
        null_ratio_.value = round(null_ratio, 4)
        return {"null_count": null_count_,"total": total_,"null_ratio": null_ratio_}


class CheckUniqueValues(BaseRule):
    def __init__(self):
        super().__init__()
    def __process__(self, input_data) -> Dict[str, ModelRes]:
    #def check_unique_values(data: Union[Dict, List], ) -> Dict[Any, Any]:
        """
        检查唯一值，包含NULL（None/空字符串）作为唯一值计入。
        返回：唯一值行数，总行数，唯一值率
        """
        data=input_data[0]['data']
        seen = set()
        unique_count = 0
        for item in data:
            if item not in seen:
                seen.add(item)
                unique_count += 1
        total = len(data)
        unique_ratio = unique_count / total if total > 0 else 0.0
        #return unique_count, total, round(unique_ratio, 4)
        unique_count_ = ModelRes()
        unique_count_.value = unique_count
        total_ = ModelRes()
        total_.value = total
        unique_ratio_ = ModelRes()
        unique_ratio_.value = round(unique_ratio, 4)
        return {
            "unique_count": unique_count_,
            "total": total_,
            "unique_ratio": unique_ratio_
        }


class CheckDuplicateValues(BaseRule):
    def __init__(self):
        super().__init__()
    def __process__(self, input_data) -> Dict[str, ModelRes]:
#def check_duplicate_values(data: Union[Dict, List], path: List[str]) -> Dict[Any, Any]:
        """
        检查重复值行数（不含NULL），重复值率。
        返回：重复值行数，总行数，重复值率
        """
        data=input_data[0]['data']
        value_counts = defaultdict(int)
        for item in data:
            if item is not None and item.strip() != "":
                value_counts[item] += 1

        duplicate_rows = sum(v for v in value_counts.values() if v > 1)
        total = len(data)
        duplicate_ratio = duplicate_rows / total if total > 0 else 0.0
        #return duplicate_rows, total, round(duplicate_ratio, 4)
        duplicate_count_ = ModelRes()
        duplicate_count_.value = duplicate_rows
        total_ = ModelRes()
        total_.value = total
        duplicate_ratio_ = ModelRes()
        duplicate_ratio_.value = round(duplicate_ratio, 4)
        return {
            "duplicate_rows": duplicate_count_,
            "total": total_,
            "duplicate_ratio": duplicate_ratio_,
        }




# if __name__ == '__main__':
#     data = csv_to_json('data.csv', 'data.json')
#     with open('data.json') as f:
#         input_data = json.load(f)[0]["data"]
#     res = RuleSpecialCharacter().eval(input_data)
#     res1 = RuleLineEndWithTerminal().eval(input_data)
#     score = comprehensive_score({'SpecialCharacter': res.value, 'ending': res1.value},
#                                 {'SpecialCharacter': 0.6, 'ending': 0.4}, strategy='weighted_sum')
#     print(score)
    