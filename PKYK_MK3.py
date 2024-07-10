import time

from opcua import Client
import csv, json, os
from datetime import datetime


class onchange_trigger():
    def __init__(self, instance):
        self.file_name = 'PokeYoke.csv'
        self.instance = instance

    def datachange_notification(self, node, val, data):
        headers = ['Time_Stamp', 'Shift_Id', 'Machine_Code', 'Variant_Code', 'Parameter_Id', 'Status', 'CompanyCode',
                   'PlantCode', 'Line_Code', 'Date']
        # print(str(node))
        if str(node).split('=')[-1] in self.instance.GEN_TAGS:
            if str(node).split('=')[-1] == 'Current_Shift':
                globals()['Current_Shift'] = val

        if str(node).split('=')[-1] in self.instance.GEN_TAGS:
            if str(node).split('=')[-1] == 'Prod_Date':
                globals()['Prod_Date'] = val
        tag = str(node.nodeid)[20:-1]
        split_tag = tag.split('_')
        if len(split_tag) == 3:
            data = [datetime.now(), globals()['Current_Shift'], split_tag[0], 'V1', tag, val,
                    self.instance.COMPANY_CODE,
                    self.instance.PLANT_CODE, self.instance.LINE_CODE, globals()['Prod_Date']]
            if os.path.exists(self.instance.FILE_PATH):
                with open(self.instance.FILE_PATH, 'a', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
                    print(f"logged {data}")
            else:
                try:
                    os.mkdir(self.instance.FOLDER_PATH)
                except:
                    pass
                with open(self.instance.FILE_PATH, 'w', newline='') as file:
                    print(f"file created {self.instance.FILE_PATH}")
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
                    print(f"logged {data}")


class onchange_trigger1():
    def __init__(self, instance):
        self.file_name = 'PokeYoke.csv'
        self.instance = instance

    def datachange_notification(self, node, val, data):
        headers = ['Time_Stamp', 'Shift_Id', 'Machine_Code', 'Variant_Code', 'Parameter_Id', 'Status', 'CompanyCode',
                   'PlantCode', 'Line_Code', 'Date']
        # print(str(node))
        if str(node).split('=')[-1] in self.instance.GEN_TAGS:
            if str(node).split('=')[-1] == 'Current_Shift':
                globals()['Current_Shift'] = val

        if str(node).split('=')[-1] in self.instance.GEN_TAGS:
            if str(node).split('=')[-1] == 'Prod_Date':
                globals()['Prod_Date'] = val
        tag = str(node.nodeid)[20:-1]
        split_tag = tag.split('_')
        if len(split_tag) == 3:
            data = [datetime.now(), globals()['Current_Shift'], split_tag[0], 'V1', tag, val,
                    self.instance.COMPANY_CODE,
                    self.instance.PLANT_CODE, self.instance.LINE_CODE, globals()['Prod_Date']]
            if os.path.exists(self.instance.FILE_PATH):
                with open(self.instance.FILE_PATH, 'a', newline='') as file:
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
                    print(f"logged {data}")
            else:
                try:
                    os.mkdir(self.instance.FOLDER_PATH)
                except:
                    pass
                with open(self.instance.FILE_PATH, 'w', newline='') as file:
                    print(f"file created {self.instance.FILE_PATH}")
                    write_log_csv = csv.writer(file)
                    write_log_csv.writerow(data)
                    print(f"logged {data}")


class initilase:
    def __init__(self, DATA):

        self.CONFIG_DATA = DATA['CONFIG_DATA']
        self.TAG_DATA = DATA['TAG_DATA']
        self.client = None
        self.ENDPOINT = self.CONFIG_DATA['end_point']
        self.LINE_CODE = self.CONFIG_DATA['line_code']
        self.PLANT_CODE = self.CONFIG_DATA['plant_code']
        self.COMPANY_CODE = self.CONFIG_DATA['company_code']
        self.FOLDER_PATH = self.CONFIG_DATA['folder_path']
        self.FILE_PATH = self.FOLDER_PATH + self.CONFIG_DATA['file_name']
        self.PKY_TAGS = self.TAG_DATA['tags']
        self.GEN_TAGS = self.TAG_DATA['general_tags']
        self.NAME_SPACE = self.CONFIG_DATA['name_space']
        self.endpoint_con_status = False
        self.connect_server()
        self.create_gen_tags()

    def create_gen_tags(self):
        for i in self.GEN_TAGS:
            globals()[i] = i

    def connect_server(self):
        try:
            self.client = Client(self.ENDPOINT)
            self.client.connect()
            self.endpoint_con_status = True
            print("::::  Connected to end point" + self.ENDPOINT)
        except:
            print(f"::::  not Connected to end point {self.ENDPOINT}")

    def attach_onchange(self):
        if self.endpoint_con_status:
            for id ,tag in enumerate(self.GEN_TAGS):
                print(id , "ID")
                cmp_tag = self.NAME_SPACE + tag
                try:
                    node = self.client.get_node(cmp_tag)
                    event_onchange = self.client.create_subscription(10, onchange_trigger(self))
                    event_onchange.subscribe_data_change(node)
                    print(f'onchange successful   {self.ENDPOINT}-{tag}')
                except Exception as e:
                    print(f'onchange unsuccessful {self.ENDPOINT}-{tag}')

            for ID ,tag in enumerate(self.PKY_TAGS):
                cmp_tag = self.NAME_SPACE + tag

                try:
                    node = self.client.get_node(cmp_tag)
                    event_onchange = self.client.create_subscription(10, onchange_trigger(self))
                    event_onchange.subscribe_data_change(node)
                    print(f'onchange successful   {self.ENDPOINT}-{tag}')
                except Exception as e:
                    print(f'onchange unsuccessful {self.ENDPOINT}-{tag}')

    def log_and_move(self):
        print("yes")


if __name__ == '__main__':
    CONFIG_DATA = json.loads(open('PKYK_config.json').read())
    for server in CONFIG_DATA:
        globals()[server] = initilase(CONFIG_DATA[server])
        globals()[server].attach_onchange()

## NOTES

# OPCUA Subscription created *
# get ewon data automatically for shift pdate variant
# timebase moving
