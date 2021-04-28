from reports.per_report import Per_report


class Factory_per_report():

    def produce(self, db_name):
        return Per_report(db_name)
