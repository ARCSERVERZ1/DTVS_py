import time

from opcua import Client
import csv, json, os
from datetime import datetime
import shutil


class opcua_monitor:
    def __init__(self, DATA):
        self.file_move_flag = True
        self.move_mins = DATA['move_mins']
        self.CONFIG_DATA = DATA['CONFIG_DATA']
        self.TAG_DATA = DATA['TAG_DATA']
        self.client = None
        self.ENDPOINT = self.CONFIG_DATA['end_point']
        self.LINE_CODE = self.CONFIG_DATA['line_code']
        self.PLANT_CODE = self.CONFIG_DATA['plant_code']
        self.COMPANY_CODE = self.CONFIG_DATA['company_code']
        self.FOLDER_PATH = self.CONFIG_DATA['folder_path'] + self.CONFIG_DATA['use_case'] + '/' + self.LINE_CODE
        self.FILE_PATH = self.FOLDER_PATH + '/' + self.CONFIG_DATA['use_case'] + '.csv'
        self.PKY_TAGS = self.TAG_DATA['tags']
        self.GEN_TAGS = self.TAG_DATA['general_tags']
        self.NAME_SPACE = self.CONFIG_DATA['name_space']
        self.endpoint_con_status = False
        self.DESTINATION_PATH = self.CONFIG_DATA['folder_path'] + self.LINE_CODE + '/' + self.CONFIG_DATA[
            'use_case'] + '/'
        self.create_tags()

        self.connect_server()

    def create_tags(self):
        for tag in self.PKY_TAGS + self.GEN_TAGS:
            globals()[tag] = None
        print('created tags')

    def connect_server(self):
        try:
            self.client = Client(self.ENDPOINT)
            self.client.connect()
            self.endpoint_con_status = True
            print("::::  Connected to end point" + self.ENDPOINT)
        except:
            print(f":::  not Connected to end point {self.ENDPOINT}")
        self.onchange_monitor()

    def onchange_monitor(self):
        while True:
            self.check_file_move()
            if self.endpoint_con_status:
                try:  # check for opcua connected
                    b = self.client.get_endpoints()
                    for ID, tag in enumerate(self.PKY_TAGS):
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
                            pass
                            # print(e)
                            # Exception.call
                except:

                    self.endpoint_con_status = False
                    print("Server disconnected while loop Broke")
                    break
            else:
                print(datetime.now(), "server not connected next try in 5 sec ")
                time.sleep(5)
                self.connect_server()

        if not self.endpoint_con_status:
            print("connected thrw out of while loop")
            self.connect_server()

    def update_gen_tags(self):
        for tag in self.GEN_TAGS:
            cmp_tag = self.NAME_SPACE + tag
            node = self.client.get_node(cmp_tag)
            globals()[tag] = node.get_value()

    def onchange_log(self, tag, data):
        self.update_gen_tags()
        split_tag = tag.split('_')
        if len(split_tag) == 3:
            data = [datetime.now(), globals()['Current_Shift'], split_tag[0], split_tag[1], tag, data,
                    self.COMPANY_CODE,
                    self.PLANT_CODE, self.LINE_CODE, globals()['Prod_Date']]
            if os.path.exists(self.FILE_PATH):
                with open(self.FILE_PATH, 'a', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
                    print(f"logged {data}")
            else:
                try:
                    os.makedirs(self.FOLDER_PATH)
                    print("New Directory created", self.FOLDER_PATH)
                except:
                    pass
                with open(self.FILE_PATH, 'w', newline='') as file:
                    print(f"file created {self.FILE_PATH}")
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
                    print(f"logged {data}")
        else:
            print('tag')

    def log_all(self):
        if self.endpoint_con_status:
            for tag in self.PKY_TAGS:
                cmp_tag = self.NAME_SPACE + tag
                try:
                    node = self.client.get_node(cmp_tag)
                    globals()[tag] = node.get_value()
                    self.onchange_log(tag, globals()[tag])
                except:
                    print(f'Failed to log tag {tag} - module> Log_all')
        else:
            print(datetime.now(), "server not connected")

    def check_file_move(self):
        current_min = int(datetime.now().second)
        if current_min in self.move_mins:
            if self.file_move_flag:
                self.log_all()
                if os.path.exists(self.FILE_PATH):
                    if not os.path.exists(self.DESTINATION_PATH): os.makedirs(self.DESTINATION_PATH)
                    file_name = self.CONFIG_DATA['use_case'] + '_' + datetime.strftime(datetime.now(),'%Y_%m_%dT%H_%M_%S') + '.csv'
                    dest_path = self.DESTINATION_PATH + file_name

                    shutil.move(self.FILE_PATH, dest_path)
                    print("file moving started")
                self.file_move_flag = False
        else:
            self.file_move_flag = True


if __name__ == '__main__':
    CONFIG_DATA = json.loads(open('PKYK_config.json').read())

    for server in CONFIG_DATA:
        globals()[server] = opcua_monitor(CONFIG_DATA[server])

# NOTES
# OPCUA Subscription created *
# get ewon data automatically for shift pdate variant
# timebase moving
