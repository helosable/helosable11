from log_analyzer.reports.per_report import Per_report


class Factory_per_report():

    def produce(self, dm):
        return Per_report(dm)
