import sqlite3
from datetime import datetime
from os import listdir
import os
import re
import json
import shutil
import pandas as pd
from Application_Logger.logger import App_Logger

class rawDataValidation:
    def __init__(self, path):
        self.batch_directory=path
        self.schema_path='schema_training.json'
        self.logger=App_Logger()

    def valuesFromSchema(self):
        """
            Validating data as per Data SHaring Agreement
        """
        try:
            with open(self.schema_path,'r') as f:
                dic=json.load(f)
                f.close()
            pattern=dic['SampleFileName']
            lengthOfDataStampInFIle=dic['LengthOfDataStampInFile']
            lengthOfTimeStampInFile=dic['LengthOfTimeStampInFIle']
            columnNames=dic['ColName']
            numberOfColumns=dic['NumberOfCOlumns']

            file=open("Training_Logs/valuesFromSchemaValidationLog.txt","a+")
            message="LengthOfDateStampInFIle:: %s" %lengthOfDataStampInFIle + "\t" + "LengthOfTimeStampFile:: %s" %lengthOfTimeStampInFile + "\t" + "NumberOfColumns:: %s" %numberOfColumns +"\n"
            self.logger.log(file,message)
            file.close()

        except ValueError:
            file=open("Training_Logs/valuesFromSchemaValidationLog.txt","a+")
            self.logger.log(file,"ValueError: Value not found inside schema_training.json")
            file.close()
            raise ValueError

        except KeyError:
            file=open("Training_Logs/valuesFromSchemaValidationLog.txt","a+")
            self.logger.log(file," KeyError: Key value error incorrect key passed")
            file.close()
            raise KeyError

        except Exception as ex:
            file=open("Training_Logs/valuesFromSchemaValidationLog.txt","a+")
            self.logger.log(file, str(ex))
            file.close()
            raise ex

        return lengthOfDataStampInFIle, lengthOfTimeStampInFile, columnNames, numberOfColumns

    def manualRegexCreation(self):
        regex="wafer+_[\d]{8}_[\d]{6}\.csv"
        return regex

    def createDirectoryForGoodBadRawData(self):
        try:
            path=os.path.join("Training_Raw_File_Validated/","Good_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)
            path=os.path.join("Training_Raw_File_Validated/","Bad_Raw/")
            if not os.path.isdir(path):
                os.mkdir(path)

        except OSError as ex:
            file=open("Training_Logs/GeneralLog.txt","a+")
            self.logger.log(file, "Error while creating directory %s:" %ex)
            file.close()
            raise OSError

    def deleteExistingGoodDataTrainingFolder(self):
        try:
            path='Training_Raw_Files_Validated/'
            if os.path.isdir(path, "Good_Raw/")
                shutil.rmtree(path+'Good_Raw/')
                file=open("Training_Logs/GeneraLog.txt","a+")
                self.logger.log(file, "GoodRaw directory deleted successfully!!!")
                file.close()
        except OSError as s:
            file=open("Training_Logs/GeneralLog.txt","a+")
            self.logger.log(file, "Error while deleting directory: %s"%s)
            file.close()
            raise OSError

    def deleteExistingBadDataTrainingFolder(self):
        try:
            path="Training_Raw_files_Validated/"
            if os.path.isdir(path+'Bad_Raw/'):
                shutil.rmtree(path+"Bad_Raw/")
                file=open("Training/Logs/GeneralLog.txt","a+")
                self.logger.log(file,"BadRaw directory deleted before starting validation")
                file.close()
        except OSError as s:
            file=open("Training_Logs/GeneralLog.txt","a+")
            self.logger.log(file,"Error while deleting directory: %s" %s)
            file.close()
            raise OSError

    def movedBadFilesToArchiveBad(self):
        now=datetime.now()
        date=now.date()
        time=now.strftime("%H::%M::%S")
        try:
            source='Training_Raw_Files_Validated/Bad_Raw/'
            if os.path.isdir(source):
                path="TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest="TrainingArchive/BadData"

