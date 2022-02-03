import sys
import json
import sqlalchemy
from odo import odo

from Source import config


class DumpDataTarget:

    def __init__(self):
        pass

    def using_odo(self,file_name, table_name, uri):
        odo(file_name, '%s::%s' % (uri, table_name))

    def get_data_csv(self):
        source_schema_file = json.load(open('Source/Source_schema.json'))
        uri = 'mysql+pymysql://' + config.username + ':' + config.password + '@' + config.server + '/' + config.database +'_invodata'
        print(uri)
        for items in source_schema_file['Query_set']:
            for item in items:
                print(item)
                try:
                    self.using_odo('/var/lib/mysql-files/'+item+'.csv',item, uri)
                except Exception as e:
                    print(e)


# if __name__ == '__main__':
#     d = DumpDataTarget()
#     d.get_data_csv()