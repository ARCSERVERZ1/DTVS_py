# Reading from json
# create seperat thrad for alerts


import DA_MK14 as dataaggregator
import threading
import os, json, time
import sys
import select
# import Visualiser_MK2

# imported_objects = globals()
HOME_PATH = 'E:/IIOT/Config/RUN_DA.json'

try:
    # print("This Program using {lib} as Core".format(lib=str(imported_objects['dataaggregator']).split(' ')[1]))
    CONFIG = json.loads(open(HOME_PATH).read())
    print(CONFIG)
    if CONFIG['run_status']:
        print("DA_MK11 as deploying....")
        IGNORE_LINE = CONFIG['IGNORE_LINE']
        LINES = []
        FOLDER_PATH = CONFIG['FOLDER_PATH']

        for i in os.listdir(FOLDER_PATH):
            if len(i.split('.')) == 1 and i not in IGNORE_LINE:
                LINES.append(i)
        print(LINES)
        for i in LINES:
            globals()['obj_' + str(i)] = dataaggregator.StartSending(
                TOKEN_URL=CONFIG['TOKEN_URL'],
                TABLE_URL=CONFIG['TABLE_URL'],
                FILE_DIR=FOLDER_PATH + i + '/',
                gen_table_list=CONFIG['gen_table_list'],
                swift_table_list=CONFIG['swift_table_list'],
                rotating_logs=CONFIG['rotating_logs'],
                py_diag=CONFIG['py_diag']
            )
# [09:51] Sahana C
            # https://tealdatalogging.azurewebsites.net/api/Values/InsertPokeYoke
            #  like 1
            globals()['obj_' + str(i)].initial_setup()
            globals()['gen_th_' + str(i)] = threading.Thread(
                target=lambda: globals()['obj_' + str(i)].sequence(CONFIG['gen_table_list'], 1))
            globals()['swift_th_' + str(i)] = threading.Thread(
                target=lambda: globals()['obj_' + str(i)].sequence(CONFIG['swift_table_list']))
            globals()['gen_th_' + str(i)].start()
            globals()['swift_th_' + str(i)].start()

    else:
        print("run_status was not 1")
except Exception as e:
    print(e)
    print('###### press X to see config details ###')
    inp =str( input() )
    if inp == 'X':
        example = {
            "IGNORE_LINE": ["Config", "SUB01"],
            "FOLDER_PATH": "E:/IIOT/",
            "TOKEN_URL": "https://tealdatalogging.azurewebsites.net/api/Values/Check_login?device_id=2123-0377-21",
            "TABLE_URL": "https://tealdatalogging.azurewebsites.net/api/Values/Insert",
            "gen_table_list": ["PokeYoke","CycleTime", "Alarm", "Loss", "Tool", "CTIndex"],
            "swift_table_list": ["RawTable", "Alert"],
            "rotating_logs": 7,
            "run_status": 1,
            "py_diag": 1
        }
        print(f"Example Json : {example}")
    print("program exited")

