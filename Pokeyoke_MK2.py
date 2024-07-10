import time

from opcua import Client
import csv, json, os
from datetime import datetime
import shutil
import csv_logs

class opcua_monitor:
    def __init__(self, DATA):
        self.csv_logger = None
        self.file_move_flag = True
        self.CONFIG_DATA = DATA['CONFIG_DATA']
        self.TAG_DATA = DATA['TAG_DATA']
        self.client = None
        self.connection_retry_time = 5
        self.ENDPOINT = self.CONFIG_DATA['end_point']
        self.MOVE_MINS = self.CONFIG_DATA['move_mins']
        self.ENABLE_LOGS = self.CONFIG_DATA['enable_logs']
        self.LINE_CODE = self.CONFIG_DATA['line_code']
        self.PLANT_CODE = self.CONFIG_DATA['plant_code']
        self.COMPANY_CODE = self.CONFIG_DATA['company_code']
        self.USE_CASE = self.CONFIG_DATA['use_case']
        self.FOLDER_PATH = self.CONFIG_DATA['folder_path'] + self.USE_CASE + '/' + self.LINE_CODE+'/'
        self.FILE_PATH = self.FOLDER_PATH + self.USE_CASE + '.csv'
        self.PKY_TAGS = self.TAG_DATA['tags']
        self.GEN_TAGS = self.TAG_DATA['general_tags']
        self.NAME_SPACE = self.CONFIG_DATA['name_space']
        self.endpoint_con_status = False
        self.DESTINATION_PATH = self.CONFIG_DATA['folder_path'] + self.LINE_CODE + '/' + self.CONFIG_DATA[
            'use_case'] + '/'
        self.create_tags()
        if self.ENABLE_LOGS:self.enable_logs()
        self.connect_server()

    def enable_logs(self):
        logs_path  = self.FOLDER_PATH+'logs/'
        self.csv_logger = csv_logs.start_logging(self.USE_CASE, logs_path)

    def create_tags(self):
        for id ,tag in enumerate(self.PKY_TAGS):
            globals()[tag] = None
            total_tags_created = id
        print(f'Total tags created : {total_tags_created+1}')
        for tag in self.GEN_TAGS:
            globals()[tag] = None


    def connect_server(self):
        try:
            self.client = Client(self.ENDPOINT)
            self.client.connect()
            self.endpoint_con_status = True
            print(f"::: End point {self.ENDPOINT} connection successful :::")
            self.csv_logger.datalog('Diagnostics', 'Connection', f'{self.ENDPOINT} connection successful')
        except:
            print(f"::: End point {self.ENDPOINT} connection unsuccessful :::")
            self.csv_logger.datalog('Diagnostics','Connection',f'{self.ENDPOINT} connection failed')
        self.onchange_monitor()

    def onchange_monitor(self):
        while True:
            self.check_file_move()
            if self.endpoint_con_status:
                try:  # check for opcua connected
                    b = self.client.get_endpoints()
                    for tag in self.PKY_TAGS:
                        cmp_tag = self.NAME_SPACE + tag
                        try:
                            node = self.client.get_node(cmp_tag)
                            if globals()[tag] == node.get_value():
                                pass
                            else:
                                globals()[tag] = node.get_value()
                                self.onchange_log(tag, globals()[tag])
                                # print("onchange:", tag, node.get_value())
                        except Exception as e:
                            # tags failed to connect
                            pass
                except:
                    self.endpoint_con_status = False
                    print("Server disconnected while loop Broke")
                    break
            else:
                print(datetime.now(), "server not connected next try in 5 sec ")
                time.sleep(self.connection_retry_time)
                self.connect_server()

        if not self.endpoint_con_status:
            self.connect_server()

    def update_gen_tags(self):
        for tag in self.GEN_TAGS:
            try:
                cmp_tag = self.NAME_SPACE + tag
                node = self.client.get_node(cmp_tag)
                globals()[tag] = node.get_value()
            except Exception as e:
                globals()[tag] = 'E100'
                self.csv_logger.datalog('Tag_Error', f'{self.NAME_SPACE + tag}', e)

    def onchange_log(self, tag, value):
        self.update_gen_tags()
        split_tag = tag.split('_')
        if len(split_tag) == 3:
            data = [datetime.now(), 'S'+str(globals()['Current_Shift']), split_tag[0], split_tag[1], tag, value,
                    self.COMPANY_CODE,
                    self.PLANT_CODE, self.LINE_CODE, globals()['Prod_Date']]
            if os.path.exists(self.FILE_PATH):
                with open(self.FILE_PATH, 'a', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)

            else:
                if not os.path.exists(self.FOLDER_PATH):
                    os.makedirs(self.FOLDER_PATH)
                    print("New Directory created", self.FOLDER_PATH)
                    self.csv_logger.datalog('File_management', f'Created', self.FOLDER_PATH)
                with open(self.FILE_PATH, 'w', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
            print(f'onchange for Tag "{tag}" and value "{value}"')
        else:
            self.csv_logger.datalog('Tag-Wrong-config', f'{split_tag}', 'Tag name wrong configuration')

    def log_all(self):
        if self.endpoint_con_status:
            for tag in self.PKY_TAGS:
                cmp_tag = self.NAME_SPACE + tag
                try:
                    node = self.client.get_node(cmp_tag)
                    globals()[tag] = node.get_value()
                    self.onchange_log(tag, globals()[tag])
                except Exception as e:
                    print(f'Failed to log tag {tag} - module> Log_all')
                    self.csv_logger.datalog('Tag_Error', f'{self.NAME_SPACE + tag}', e)
        else:
            print(datetime.now(), "server not connected")

    def check_file_move(self):
        current_min = int(datetime.now().second)
        if current_min in self.MOVE_MINS:
            if self.file_move_flag:
                # self.log_all()
                if os.path.exists(self.FILE_PATH):
                    if not os.path.exists(self.DESTINATION_PATH): os.makedirs(self.DESTINATION_PATH)
                    file_name = self.CONFIG_DATA['use_case'] + '_' + datetime.strftime(datetime.now(),
                                                                                       '%Y_%m_%dT%H_%M_%S') + '.csv'
                    dest_path = self.DESTINATION_PATH + file_name
                    shutil.move(self.FILE_PATH, dest_path)
                    print("file moved to path :",dest_path)
                self.file_move_flag = False
        else:
            self.file_move_flag = True


if __name__ == '__main__':
    CONFIG_DATA = json.loads(open('PKYK_config.json').read())

    for server in CONFIG_DATA:
        globals()[server] = opcua_monitor(CONFIG_DATA[server])

