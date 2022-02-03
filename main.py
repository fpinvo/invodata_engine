

from Source import config
from Source.s_metadata import SourceMetaData
from Target.t_metadata import TargetMetaData
from Source.fetch_data_from_source import FetchDataFromSource
from Target.dump_data_target import DumpDataTarget

class Main:
    
    def __init__(self, *args, **kwargs):
        self.get_source_meta_()
        self.dump_target_meta_()
        self.get_source_data_()
        self.dump_target_data_()
    
    def get_source_meta_(self):
        s = SourceMetaData()
        cur = s.test_connection(config.server, config.username, config.password, config.database, config.port)
        source_table = s.get_table_names(cur, config.database)
        source_metadata = s.get_metadata(source_table)
        source_table.close()
        cur.close()
    
    def dump_target_meta_(self):
        t = TargetMetaData()
        cur = t.test_connection(config.server, config.username, config.password, config.database, config.port)
        t.create_db(cur)
        target_table = t.create_table(cur)
        target_table.close()
        cur.close()
        
    def get_source_data_(self):
        f_data = FetchDataFromSource()
        f_data_con = f_data.test_connection(config.server, config.username, config.password, config.database, config.port)
        f_data.fetch_tables(f_data_con)
    
    def dump_target_data_(self):
        d = DumpDataTarget()
        d.get_data_csv()
        
        
if __name__ == '__main__':
    main = Main()