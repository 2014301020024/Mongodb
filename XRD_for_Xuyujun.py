import datetime
import pymongo
import pandas as pd
import shutil
import os
from time import sleep
import numpy as np
import getpass
import pyautogui

def Link_Mongo():
    username = input("Username:")
    position = pyautogui.position()
    pyautogui.click(x=position[0], y=position[1])
    pyautogui.hotkey('command', 'l')
    password = input("Password:")
    position = pyautogui.position()
    pyautogui.click(x=position[0], y=position[1])
    pyautogui.hotkey('command', 'l')
    position = pyautogui.position()
    pyautogui.click(x=position[0], y=position[1])
    port = input("Connection Port:")
    pyautogui.hotkey('command', 'l')
    try:
        connection = pymongo.MongoClient('mongodb://' + username + ':' + password + '@' + port + '/webdev', serverSelectionTimeoutMS=5)
        db_XRD = connection.XRD
        db_XRD.Xrd.find_one()
    except Exception:
        raise Exception('Please check your account, password and port.')
    return db_XRD

class XRD():
    def __init__(self):
        self.dirname = os.path.dirname(__file__)
        self.XRD = Link_Mongo()
        self.Xrd = self.XRD.XRD
        self.Libraries = self.XRD.Libraries
        self.Synthesis = self.XRD.Synthesis
        return

    def get_Sample_name_and_id(self):
        if list(self.Synthesis.find()) != []:
            for doc in self.Synthesis.find().sort('Sample_id', -1):
                Max_Sample_id_in_db = doc["Sample_id"]
                Sample_id = Max_Sample_id_in_db[:3] + str(int(Max_Sample_id_in_db[3:])).zfill(7)
                break
        else:
            raise Exception('There is no sample in the database.')
        Sample_name = self.Synthesis.find_one({"Sample_id": Sample_id})["Sample_name"]
        return Sample_id, Sample_name

    def Synthesis_condition(self, Sample_id=None):
        '''a function to write PLD_deposition_film_parameters(Synthesis_conditions) into database XRD'''
        dirs_Synthesis = os.listdir(os.path.join(os.path.dirname(self.dirname), 'Synthesis'))
        dirs_Synthesis = [file for file in dirs_Synthesis if file[0] !="."]
        if len(dirs_Synthesis) != 1:
            raise Exception('The number of files is %.0f, not required 1' %len(dirs_Synthesis))
        Synthesis_path = os.path.join(os.path.dirname(self.dirname), 'Synthesis/' + dirs_Synthesis[0])
        time_begin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("# " + str(time_begin) + ": Start to open the file of Synthesis_condition!")
        File_Dataframe = pd.read_csv(Synthesis_path, sep=":", header=None)
        File_Dataframe.columns = ['parameter', 'value']
        Useful_Dataframe = File_Dataframe[~File_Dataframe.isna().any(axis=1)]
        Parameters = list(Useful_Dataframe.parameter)
        Values = list(Useful_Dataframe.value)
        if Sample_id:
            if self.Synthesis.find_one({"Sample_id": Sample_id}) is not None:
                Answer = input("Rewrite the sample Synthesis condition of " + Sample_id + "(Y or N):")
                if Answer.upper() in ["Y", "YE", "YES"]:
                    self.Synthesis.delete_one({"Sample_id": Sample_id})
                    Synthesis_doc = {}
                    Synthesis_doc["Sample_id"] = Sample_id
                    Sample_name = dirs_Synthesis[0].split(".")[0].split("_")[0].split("-")[0]
                    Synthesis_doc["Sample_name"] = Sample_name
                    print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name + ".")
                    Step_1, Step_2, Step_3 = {}, {}, {}
                    for index, Parameter in enumerate(Parameters):
                        if '(STEP 1)' in Parameter:
                            Step_1[Parameter[:Parameter.find('(STEP 1)')]] = Values[index]
                        elif '(STEP 2)' in Parameter:
                            Step_2[Parameter[:Parameter.find('(STEP 2)')]] = Values[index]
                        elif '(STEP 3)' in Parameter:
                            Step_3[Parameter[:Parameter.find('(STEP 3)')]] = Values[index]
                        else:
                            Synthesis_doc[Parameter] = Values[index]
                    Synthesis_doc['STEP_1'], Synthesis_doc['STEP_2'], Synthesis_doc['STEP_3'] = Step_1, Step_2, Step_3
                    Synthesis_doc['datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.Synthesis.insert_one(Synthesis_doc)
                    # shutil.move(Synthesis_path, os.path.join(os.path.dirname(self.dirname), 'DONE/'))
                    time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("# " + str(time_end) + ": The synthesis_condition of " + Sample_id  + " has been modified.")
                    print(" ")
                else:
                    print("-> " + dirs_Synthesis[0] + " was passed.")
                    time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    print("# " + str(time_end) + ": The synthesis_condition of " + Sample_id  + " didn\'t change.")
                    print(" ")
                    return "XRD.Synthesis"
            else:
                raise Exception('This Sample_id does not yet exist in the database！This means that you don\'t need to '
                                'add the Sample_id parameter yourself, and run the program after removing it, '
                                'and the Sample_id will be generated by itself.')
        else:
            Synthesis_doc = {}
            if list(self.Synthesis.find()) != []:
                for doc in self.Synthesis.find().sort('Sample_id', -1):
                    Max_Sample_id_in_db = doc["Sample_id"]
                    break
                Sample_id = Max_Sample_id_in_db[:3] + str(int(Max_Sample_id_in_db[3:]) + 1).zfill(7)
                if self.Synthesis.find_one({"Sample_id": Sample_id}) is None:
                    Synthesis_doc["Sample_id"] = Sample_id
            else:
                Sample_id = 'Sp-0000001'
                if self.Synthesis.find_one({"Sample_id": Sample_id}) is None:
                    Synthesis_doc["Sample_id"] = Sample_id
            Sample_name = dirs_Synthesis[0].split(".")[0].split("_")[0].split("-")[0]
            Synthesis_doc["Sample_name"] = Sample_name
            print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name +".")
            Step_1, Step_2, Step_3 = {}, {}, {}
            for index, Parameter in enumerate(Parameters):
                if '(STEP 1)' in Parameter:
                    Step_1[Parameter[:Parameter.find('(STEP 1)')]] = Values[index]
                elif '(STEP 2)' in Parameter:
                    Step_2[Parameter[:Parameter.find('(STEP 2)')]] = Values[index]
                elif '(STEP 3)' in Parameter:
                    Step_3[Parameter[:Parameter.find('(STEP 3)')]] = Values[index]
                else:
                    Synthesis_doc[Parameter] = Values[index]
            Synthesis_doc['STEP_1'], Synthesis_doc['STEP_2'], Synthesis_doc['STEP_3'] = Step_1, Step_2, Step_3
            Synthesis_doc['datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.Synthesis.insert_one(Synthesis_doc)
            # shutil.move(Synthesis_path, os.path.join(os.path.dirname(self.dirname), 'DONE/'))
            time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print("# " + str(time_end)+ ": Synthesis_condition was done for XRD.Synthesis.")
            print(" ")
        return "XRD.Synthesis"

    def Xrd_data(self, measurement, Sample_id=None):
        total_measurement = ['PHASE_ANALYSIS', 'PHI_SCAN', 'ROCKING_CURVE', 'RECIPROCAL_SPACE_METHOD', 'GIWAXS', 'FILM_THICKNESS']
        measurement = measurement.upper()
        if measurement not in total_measurement:
            print("The measurement was wrong, exactly should be such as 'PHASE_ANALYSIS'.")
            Answer_mes = input("The measurement:")
            if Answer_mes.upper() in total_measurement:
                measurement = Answer_mes.upper()
            else:
                raise Exception("The measurement was still wrong, please make sure!")
        Xrd_path = os.path.join(os.path.dirname(self.dirname), 'Xrd')
        dirs_Xrd = os.listdir(Xrd_path)
        dirs_Xrd = [file for file in dirs_Xrd if file[0] !="."]
        time_begin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("# " + str(time_begin) + ": Start open the file of Xrd corresponding to Sample!")
        if Sample_id:
            if self.Synthesis.find_one({"Sample_id": Sample_id}) is not None:
                Sample_name = self.Synthesis.find_one({"Sample_id": Sample_id})["Sample_name"]
            else:
                raise Exception('This Sample_id does not yet exist in the database!')
        else:
            Sample_id, Sample_name = self.get_Sample_name_and_id()
        print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name + ".")
        Xrd_id_c = []
        for dirc in dirs_Xrd:
            dirc_s = dirc.split(".")[0].split("_")
            x_local, y_local = dirc_s[1], dirc_s[-1]
            Xrd_id = 'Xrd-' + x_local + "-" + y_local
            Xrd_doc = {}
            Xrd_doc['Sample_id'] = Sample_id
            Xrd_doc['Sample_name'] = Sample_name
            if self.Xrd.find_one({"Sample_id": Sample_id, "Sample_name": Sample_name, "Measurement": measurement,
                                  "Xrd_id": Xrd_id}) is not None:
                Answer = input("Rewrite the XRD data of " + Xrd_id + "(Y or N):")
                if Answer.upper() in ["Y", "YE", "YES"]:
                    self.Xrd.delete_one({"Sample_id": Sample_id, "Sample_name": Sample_name, "Measurement": measurement, "Xrd_id": Xrd_id})
                    Xrd_doc['Xrd_id'] = Xrd_id
                    Xrd_doc['x_local'] = x_local
                    Xrd_doc['y_local'] = y_local
                    Xrd_Data = pd.read_csv(os.path.join(Xrd_path, dirc), sep=" ", header=None)
                    Xrd_Data.columns = ["parameter", "value"]
                    parameter = list(Xrd_Data.parameter)
                    value = list(Xrd_Data.value)
                    for index, para in enumerate(parameter):
                        if 48 <= ord(para[0]) <= 57 and (48 <= ord(para[1]) <= 57 or ord(para[1]) == 46):
                            Index_start = index
                            break
                    Xrd_x, Xrd_y = parameter[Index_start:], value[Index_start:]
                    Xrd_pattern = {}
                    Xrd_pattern["2Theta"], Xrd_pattern["Strength"] = Xrd_x, Xrd_y
                    Xrd_doc['Measurement'] = measurement.upper()
                    Xrd_doc['pattern'] = Xrd_pattern
                    Xrd_doc['datetime'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    self.Xrd.insert_one(Xrd_doc)
                    # shutil.move(os.path.join(Xrd_path, dirc), os.path.join(os.path.dirname(self.dirname), 'DONE/'))
                    print("-> " + dirc + " was imported into XRD.Xrd successfully.")
                    print("# "+ "The Xrd data of " + Xrd_id  + "has been modified.")
                else:
                    print("-> " + dirc + " was passed.")
                    continue
            else:
                Xrd_doc['Xrd_id'] = Xrd_id
                Xrd_doc['x_local'] = x_local
                Xrd_doc['y_local'] = y_local
                Xrd_id_c.append(Xrd_id)
                Xrd_Data = pd.read_csv(os.path.join(Xrd_path, dirc), sep=" ", header=None)
                Xrd_Data.columns = ["parameter", "value"]
                parameter = list(Xrd_Data.parameter)
                value = list(Xrd_Data.value)
                for index, para in enumerate(parameter):
                    if 48 <= ord(para[0]) <= 57 and (48 <=ord(para[1]) <= 57 or ord(para[1]) == 46):
                        Index_start = index
                        break
                Xrd_x, Xrd_y = parameter[Index_start:], value[Index_start:]
                Xrd_pattern = {}
                Xrd_pattern["X"], Xrd_pattern["Y"] = Xrd_x, Xrd_y
                Xrd_doc['Measurement'] = measurement.upper()
                Xrd_doc['pattern'] = Xrd_pattern
                Xrd_doc['date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                self.Xrd.insert_one(Xrd_doc)
                # shutil.move(os.path.join(Xrd_path, dirc), os.path.join(os.path.dirname(self.dirname), 'DONE/'))
                print("-> "  + dirc + " was imported into XRD.Xrd successfully.")
                print("-> The Xrd data of " + Xrd_id + ' has been created.')

        doc_lib = self.Libraries.find_one({"Sample_id": Sample_id, "Sample_name": Sample_name})
        if doc_lib is not None and measurement in doc_lib.keys():
            patterns = sorted(doc_lib[measurement]["patterns"] + Xrd_id_c)
            time_later = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.Libraries.update_one({"Sample_id": Sample_id},
                                      {'$set': {measurement +'.patterns': patterns, measurement + '.datetime': time_later}})
            print("-> Finnished Updating the Library of " + Sample_id + ".")
        time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("# " + str(time_end) + ": All Xrd data files were done for XRD.Xrd.")
        print(" ")
        return 'Xrd_data'

    def Xrd_library(self, measurement, Sample_id=None):
        total_measurement = ['PHASE_ANALYSIS', 'PHI_SCAN', 'ROCKING_CURVE', 'RECIPROCAL_SPACE_METHOD', 'GIWAXS', 'FILM_THICKNESS']
        measurement = measurement.upper()
        if measurement not in total_measurement:
            print("The measurement was wrong, exactly should be such as 'PHASE_ANALYSIS'.")
            Answer_mes = input("The measurement:")
            if Answer_mes.upper() in total_measurement:
                measurement = Answer_mes.upper()
            else:
                raise Exception("The measurement was still wrong, please make sure!")
        library_path = os.path.join(os.path.dirname(self.dirname), 'Library')
        dirs_library = os.listdir(library_path)
        dirs_library = [file for file in dirs_library if file[0] !="."]
        if len(dirs_library) != 1:
            raise Exception('The number of files is %.0f, not required ' % len(dirs_library))
        time_begin = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("# " + str(time_begin) + ": Start to open the file of library corresponding to Sample!")
        library_path = os.path.join(library_path, dirs_library[0])
        File_Dataframe = pd.read_csv(library_path, sep=":", header=None)
        File_Dataframe.columns = ['parameter', 'value']
        Useful_Dataframe = File_Dataframe[~File_Dataframe.isna().any(axis=1)]
        Parameters = list(Useful_Dataframe.parameter)
        Values = list(Useful_Dataframe.value)
        fix_parameter = ['USERGROUP', 'OPERATOR', 'SPACE GROUP', 'LATTICE CONSTANT a', 'LATTICE CONSTANT b',
                         'LATTICE CONSTANT c', 'ALPHA', 'BETA', 'GAMA']
        if Sample_id:
            if self.Synthesis.find_one({"Sample_id": Sample_id}) is not None:
                if self.Libraries.find_one({"Sample_id": Sample_id}) is not None:
                    Answer = input("Do you want to update a measurement_content or modify the entire Xrd parameter?（1.U, 2:M):")
                    if Answer.upper() == "U":
                        Xrd_library_doc = self.Libraries.find_one({"Sample_id": Sample_id})
                        Sample_name = Xrd_library_doc["Sample_name"]
                        print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name + ".")
                        print('-> The measurement content you want to add is ' + measurement + ".")
                        measurement_doc = {}
                        for index, parameter in enumerate(Parameters):
                            if parameter in fix_parameter:
                                continue
                            else:
                                measurement_doc[parameter] = Values[index]
                        Xrd_docs = self.Xrd.find({"Sample_id": Sample_id, "Measurement": measurement})
                        Xrd_ids = [Xrd_doc['Xrd_id'] for Xrd_doc in Xrd_docs]
                        measurement_doc['patterns'] = Xrd_ids
                        measurement_doc["datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        self.Libraries.update_one({"Sample_id": Sample_id}, {"$set": {measurement: measurement_doc}})
                        print('-> The ' + measurement + ' had been add to the library of ' + Sample_id + '.')
                    elif Answer.upper() == "M":
                        Xrd_library_doc = self.Libraries.find_one({"Sample_id": Sample_id})
                        Sample_name = Xrd_library_doc["Sample_name"]
                        self.Libraries.delete_one({"Sample_id": Sample_id})
                        print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name + ".")
                        Xrd_library_doc = {}
                        Xrd_library_doc['Sample_id'] = Sample_id
                        Xrd_library_doc['Sample_name'] = Sample_name
                        measurement_doc = {}
                        for index, parameter in enumerate(Parameters):
                            if parameter in fix_parameter:
                                Xrd_library_doc[parameter] = Values[index]
                            else:
                                measurement_doc[parameter] = Values[index]
                        Xrd_docs = self.Xrd.find({"Sample_id": Sample_id, "Measurement": measurement})
                        Xrd_ids = [Xrd_doc['Xrd_id'] for Xrd_doc in Xrd_docs]
                        measurement_doc['patterns'] = Xrd_ids
                        measurement_doc["datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        Xrd_library_doc[measurement] = measurement_doc
                        self.Libraries.insert_one(Xrd_library_doc)
                        print("-> " + dirs_library[0] + " was imported into XRD.Library successfully.")
                        print("-> " + "The Library data of " + Sample_id + "has been modified.")
                    else:
                        print("-> " + dirs_library[0] + " was passed.")
                        print("-> The library of " + Sample_id + " didn\'t change.")
                        print(" ")
                        return 'Xrd_Library'
                else:
                    Xrd_library_doc = {}
                    Xrd_library_doc['Sample_id'] = Sample_id
                    Synthesis_doc = self.Synthesis.find_one({"Sample_id": Sample_id})
                    Sample_name = Synthesis_doc["Sample_name"]
                    Xrd_library_doc['Sample_name'] = Sample_name
                    print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name + ".")
                    measurement_doc = {}
                    for index, parameter in enumerate(Parameters):
                        if parameter in fix_parameter:
                            Xrd_library_doc[parameter] = Values[index]
                        else:
                            measurement_doc[parameter] = Values[index]
                    Xrd_docs = self.Xrd.find({"Sample_id": Sample_id, "Measurement": measurement})
                    Xrd_ids = [Xrd_doc['Xrd_id'] for Xrd_doc in Xrd_docs]
                    measurement_doc['patterns'] = Xrd_ids
                    measurement_doc["datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    Xrd_library_doc[measurement] = measurement_doc
                    self.Libraries.insert_one(Xrd_library_doc)
                    print("-> " + dirs_library[0] + " was imported into XRD.Library successfully.")
                    print("-> " + "The Library data of " + Sample_id + " has been created.")
            else:
                raise Exception("There is no sample whose id is " + Sample_id +" in XRD database, why is there a corresponding parameter for measuring Xrd?")
        else:
            Sample_id, Sample_name = self.get_Sample_name_and_id()
            Xrd_library_doc = {}
            Xrd_library_doc['Sample_id'] = Sample_id
            Xrd_library_doc['Sample_name'] = Sample_name
            print('-> The Sample_id, Sample_name are ' + Sample_id + ", " + Sample_name + ".")
            measurement_doc = {}
            for index, parameter in enumerate(Parameters):
                if parameter in fix_parameter:
                    Xrd_library_doc[parameter] = Values[index]
                else:
                    measurement_doc[parameter] = Values[index]
            Xrd_docs = self.Xrd.find({"Sample_id": Sample_id, "Measurement": measurement})
            Xrd_ids = [Xrd_doc['Xrd_id'] for Xrd_doc in Xrd_docs]
            measurement_doc['patterns'] = Xrd_ids
            measurement_doc["datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            Xrd_library_doc["datetime"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            Xrd_library_doc[measurement] = measurement_doc
            self.Libraries.insert_one(Xrd_library_doc)
            print("-> " + dirs_library[0] + " was imported into XRD.Library successfully.")
            print("-> " + "The Library data of " + Sample_id + "has been created.")
        # shutil.move(os.path.join(Xrd_library_path, dirs_library[0]), os.path.join(os.path.dirname(self.dirname), 'DONE/'))
        time_end = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print("# " + str(time_end) + ": Library was done for Xrd.Library.")
        print(" ")
        return 'Xrd_Library'

    def Write_into_database_XRD(self, measurement):
        print('<<<<<<<<-------         imported data: start        ------->>>>>>>>', "\n")
        self.Synthesis_condition(), self.Xrd_data(measurement), self.Xrd_library(measurement)
        print('<<<<<<<<-------         imported data: end        ------->>>>>>>>', "\n")
        return
# --------------------------------------------------------------------------------------------------------
