import traceback
from abc import abstractmethod
from core.core import get_logger


class BaseTransformLoad:
     # BaseModel: typing.Type[BM]
    #
    # def __init__(self, Model: typing.Type[BM]):
    #     BaseExtractor.BaseModel = Model
    # def __init__(self):

    is_processing = False

    def whoami(self):
        print(type(self).__name__)

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def bulk_load(self, records: list[dict]):
        pass

    @abstractmethod
    def init_table(self):
        pass

    def process(self):
        try:
            if BaseTransformLoad.is_processing:
                get_logger().info(self.whoami() + " - is processing")
                return
            BaseTransformLoad.is_processing = True
            self.init_table()
            frame = self.transform()
            offset = int(1e5)
            for pointer in range(0, len(frame), offset):
                self.bulk_load(frame.loc[pointer: pointer + offset])

        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()
        finally:
            BaseTransformLoad.is_processing = False

