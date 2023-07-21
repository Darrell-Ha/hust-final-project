import traceback
from abc import abstractmethod
from core.core import get_logger


class BaseExtractor:

    is_processing = False

    def whoami(self):
        print(type(self).__name__)

    @abstractmethod
    def fetch(self):
        pass

    @abstractmethod
    def extract(self):
        pass
    
    @abstractmethod
    def bulk_upsert(self):
        pass

    @abstractmethod
    def init_table(self):
        pass

    @abstractmethod
    def process(self):
        try:
            if BaseExtractor.is_processing:
                get_logger().info(self.whoami() + " - is processing")
                return
            BaseExtractor.is_processing = True
            records = self.extract()
            self.init_table()
            offset = int(1e5)
            for pointer in range(0, len(records), offset):
                self.bulk_upsert(records[pointer: pointer+offset])

        except Exception as e:
            get_logger().error(e)
            traceback.print_exc()
        finally:
            BaseExtractor.is_processing = False

