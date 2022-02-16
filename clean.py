import atexit
import gc
import logging
import signal
import sys
import weakref

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Observer:
    """退出时对链接进行清理"""
    def __init__(self):
        self.collect = weakref.WeakSet()

        atexit.register(self.cleanup)
        # 程序终止(interrupt)信号, 在用户键入INTR字符(通常是Ctrl+C)时发出，用于通知前台进程组终止进程
        signal.signal(signal.SIGINT, self.cleanup)
        # 程序结束(terminate)信号, 与SIGKILL不同的是该信号可以被阻塞和处理。通常用来要求程序自己正常退出，shell命令kill缺省产生这个信号。如果进程终止不了，我们才会尝试SIGKILL
        signal.signal(signal.SIGTERM, self.cleanup)
        # 本信号在用户终端连接(正常或非正常)结束时发出, 通常是在终端的控制进程结束时
        signal.signal(signal.SIGHUP, self.cleanup)

    def add(self, obj):
        weakref.finalize(obj, self.cleanup, methods="Normal")
        self.collect.add(obj)

    def cleanup(self, methods=None, *args, **kwargs):
        for obj in self.collect:
            logger.warning(f"退出，清理引用对象{obj}")
            obj.clean_pool()

        self.collect = weakref.WeakSet()
        gc.collect()
        sys.exit()


observer = Observer()
