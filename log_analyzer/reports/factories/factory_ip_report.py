from log_analyzer.reports.ip_report import Ip_report


class Factory_ip_report():

    def produce(self, dm):
        return Ip_report(dm)
