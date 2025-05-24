"""通过这个模块设置组件的简名或别名，方便流程yaml中使用"""
from quality_filter.loader.qadata import QadataJsonDump, QadataXmlIncr

base2 = "quality_filter.loader"

components = {
    f"{base2}.QadataJsonDump": QadataJsonDump,
    f"{base2}.QadataXmlIncr": QadataXmlIncr
}
