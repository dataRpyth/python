import os
import sys
from os.path import join as join_path

cur_path = os.path.dirname(os.path.realpath(__file__))
sys.path.append(join_path(cur_path, ''))


class DdtDiskSdkConf:

    @classmethod
    def get_jar_path(cls):
        return join_path(cur_path, 'ddt-cspace.jar')

    @classmethod
    def get_class_list(cls):
        return ['cn.oyohotels.ddt.espace.CspaceApp']

    @classmethod
    def get_class_cspace_app(cls):
        return 'cn.oyohotels.ddt.espace.CspaceApp'
