import json
import snap7
import sys
sys.path.insert(0, "../..")
import time
from opcua import ua, Server

# DB1090F238,ISOTCP,192.168.46.1,03.01
class runserver:

    def __init__(self, opcua_server_ip, opcua_server_port, plc_config_json, diag_print):

        self.opcua_server_ip = opcua_server_ip
        self.opcua_server_port = opcua_server_port
        self.diag_print = diag_print

        try:
            with open('config.json', 'r') as file:
                self.plc_config_data = json.load(file)

        except Exception as e:
            print(e)

    def connect_plc(self):
        # multiple plc code not written yet
        for plc in self.plc_config_data:
            plc_details = self.plc_config_data[plc]['plc_details']
            try:
                globals()[plc] = snap7.client.Client()
                globals()[plc].connect(plc_details[0], int(plc_details[1]), int(plc_details[2]))

                print(f"PLC Connection successful for PLC {plc}")
                return True
            except Exception as e:
                if self.diag_print: print(e)
                print(f"PLC Connection failed for PLC {plc}")
                return False

    def read_plc_data(self, plc, tag):
        tag_data = self.plc_config_data[plc]['tags'][tag]
        tag_value = snap7.util.get_int(globals()[plc].db_read(int(tag_data[1]), 0, 250), int(tag_data[2]))
        return tag_value

    def create_server(self):
        if self.connect_plc():
            server = Server()
            # set_point = "opc.tcp://" + self.opcua_server_ip + ":" + self.opcua_server_port + "/"
            set_point = "opc.tcp://0.0.0.0:4840/freeopcua/opcua_server/"

            server.set_endpoint(set_point)
            uri = "http://examples.freeopcua.github.io"
            idx = server.register_namespace(uri)
            objects = server.get_objects_node()
            server_obj = objects.add_object(idx, "OPCUA-SANJAY")
            print("OPCUA ENDPOINT:"+ set_point)
            print("creating all snap7 tags as to OPCUA Tags ")
            for plc in self.plc_config_data:
                for tag in self.plc_config_data[plc]['tags']:
                    tag_value = self.read_plc_data(plc, tag)
                    globals()[tag] = server_obj.add_variable(idx, tag, tag_value)
                    globals()[tag].set_writable()
                    print(f"Tag name = {tag} , opcua_id = {globals()[tag].nodeid} , Tag_value is {tag_value}")
            server.start()
            time.sleep(2)
            while True:
                for plc in self.plc_config_data:
                    for tag in self.plc_config_data[plc]['tags']:
                        tag_value = self.read_plc_data(plc, tag)
                        globals()[tag].set_value(tag_value)
                        print(f"Tag name = {tag} Tag_value is {tag_value}")
                    time.sleep(2)


if __name__ == '__main__':
    server = runserver('192.168.0.1', "4840", 'config.json', True).create_server()
