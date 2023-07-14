import os
import sys
from os.path import join as join_path

import jpype

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, ''))


class DdtJvmWrapper:
    _CLAZZ_MAP = {}

    def __init__(self, jar_path, class_entry_list):
        if not jpype.isJVMStarted():
            jvm_path = jpype.getDefaultJVMPath()  # jvmPath = ur'D:\jre-8u151-windows-i586\jre1.8.0_151\bin\client\jvm.dll'
            jpype.startJVM(jvm_path, "-ea", "-Djava.class.path=%s" % jar_path)
        jpype.attachThreadToJVM()
        self.clazz_map = {}
        for class_entry in class_entry_list:
            jvm_class = jpype.JClass(class_entry)
            self.clazz_map[class_entry] = jvm_class

    @staticmethod
    def init_clazz_map(jar_path, class_entry_list):
        jvm_path = jpype.getDefaultJVMPath()  # jvmPath = ur'D:\jre-8u151-windows-i586\jre1.8.0_151\bin\client\jvm.dll'
        jpype.startJVM(jvm_path, "-ea", "-Djava.class.path=%s" % jar_path)
        clazz_map = {}
        for class_entry in class_entry_list:
            jvm_class = jpype.JClass(class_entry)
            clazz_map[class_entry] = jvm_class
        DdtJvmWrapper._CLAZZ_MAP = clazz_map
        return clazz_map

    @staticmethod
    def set_jvm_class(clazz_map):
        DdtJvmWrapper._CLAZZ_MAP = clazz_map

    @staticmethod
    def get_jvm_class(clazz_entry):
        if DdtJvmWrapper._CLAZZ_MAP.get(clazz_entry) is None:
            DdtJvmWrapper._CLAZZ_MAP[clazz_entry] = jpype.JClass(clazz_entry)
        return DdtJvmWrapper._CLAZZ_MAP.get(clazz_entry)

    def get_class(self, clazz_entry):
        if self.clazz_map.get(clazz_entry) is None:
            self.clazz_map[clazz_entry] = jpype.JClass(clazz_entry)
        return self.clazz_map.get(clazz_entry)

    def __del__(self):
        # jpype.shutdownJVM()
        pass
