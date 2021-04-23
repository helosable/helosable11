from ip_report import Ip_report

class Factory_ip_report():
    def produce(self, db_name):
        return Ip_report(db_name)