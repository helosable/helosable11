import os
import sys
sys.path.append(os.getcwd())
from log_analyzer.models.factories.ip_report import Ip_report


class Factory_ip_report():

    def produce(self, db_name):
        return Ip_report(db_name)
