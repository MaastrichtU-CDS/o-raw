# -*- coding: utf-8 -*-
"""
Created on Fri Mar 23 07:32:46 2018

@author: zhenwei.shi
"""

"""
###############################
@author: zhenwei.shi, Maastro##
###############################
"""
#from __future__ import print_function

import PyrexReader
import PyrexWithParams
import PyrexOutput
import yaml
from PyrexXNAT import ParseStructure, xnat_collection
import logging
import os
import pandas
import radiomics
import re
from rdflib import Graph
import time
import uuid
'''
Usage for individual case: python HelloPyrexBatchProcessing.py 

Read parameter file of Pyrex:

# - path:
#    - myWorkingDirectory is the root directory where DICOM files are saved.
#    - exportDir is the directory where results are exported.
# - collectionURL: specify the URL of cloud repository, like 'XNAT'.
# - myProject: specify the name of dataset on cloud reposity, like 'stwstrategyrdr'
# - export_format: specify the format of output, such as rdf or csv.
# - export_name: specify the name of result file.
######################################
'''
def executeORAWbatch_roi(myWorkingDirectory,roi,myStructUID,exportDir,export_format,export_name):
    outPath = r''
    logger = logging.getLogger('radiomics')

    # Set verbosity level for output to stderr (default level = WARNING)
    #radiomics.setVerbosity(logging.INFO)

    logger.info('pyradiomics version: %s', radiomics.__version__)
    logger.info('Reading Params file for pyradiomics')
    # Reading Params file for pyradiomics
    try:
    	paramsFile = os.path.join(os.getcwd(),'ParamsSettings','Pyradiomics_Params.yaml')
    except Exception:
      logger.error('Could not find params file of Pyradiomics!', exc_info=True)
      exit(-1)
  #  logger.info('Reading Params file of Pyrex')

    patient = os.listdir(myWorkingDirectory)
    #xnat_collection(myWorkingDirectory,collectionURL,myProjectID)
    Img_path,RT_path = ParseStructure(myWorkingDirectory) #detect the path of Image and RTstruct
    logger.info('DICOM and RTSTRUCT Parsing Done')

    flists = pandas.DataFrame(data= {'patient':patient}).T # create a pandas data frame for data
    logger.info('Starting Pyrex')

    # define export output format
    if export_format == 'csv':
      RESULT = pandas.DataFrame()
    else:
      RESULT = Graph()

    for entry in flists:
        logger.info('Processing Radiomics on %s of Patient (%s)', patient[entry])
        results = pandas.DataFrame()
#        logger.info('processing patient: %s', patient[entry])
        mask_vol=PyrexReader.Read_RTSTRUCT(RT_path[entry])
        logger.info('Loading RTSTRUCT: %s', RT_path[entry])
        M=mask_vol[0]
        target = []
        for j in range(0,len(M.StructureSetROISequence)):
            target.append(M.StructureSetROISequence[j].ROIName)
        logger.info('ROI: %s', target)

        if roi in target:
            try:
                featureVector = flists[entry]
                #featureVector['patient'] = featureVector['patient'] + 'Pyrex_' + target[k]
                Image,Mask = PyrexReader.Img_Bimask(Img_path[entry],RT_path[entry],roi)
                logger.info('Processing Radiomics on %s of Patient (%s)',roi,patient[entry])
                if export_format == 'csv': # sava results in csv
                    try:
                        result = pandas.Series(PyrexWithParams.CalculationRun(Image,Mask,paramsFile))
                        contour = pandas.Series({'contour':roi})
                        structUID = pandas.Series({'structUID':myStructUID})
                        featureVector = featureVector.append(contour)
                        featureVector = featureVector.append(structUID)
                        featureVector = featureVector.append(result)
                        featureVector.name = 1
                        results = results.join(featureVector, how='outer')
                        Image = []
                        Mask= []
                        result=[]
                        RESULT = pandas.concat([RESULT,results],axis=1)
                    except Exception:
                      logger.error('FEATURE EXTRACTION FAILED for CSV output: %s' % e, exc_info=True)
                else:# save results in triple stroe
                    try:
                      featureVector = PyrexWithParams.CalculationRun(Image,Mask,paramsFile) #compute radiomics
                      featureVector.update({'patient':patient[entry],'contour':target[k],'RTid':myStructUID}) #add patient ID and contour
                      graph_roi = PyrexOutput.RadiomicsRDF(featureVector,exportDir,patient[entry],myStructUID,target[k],export_format,export_name) #store radiomics locally with a specific format
                      RESULT = RESULT + graph_roi
                      logger.info('Extraction complete, writing rdf')
                    except Exception:
                      logger.error('FEATURE EXTRACTION FAILED for RDF output: %s' % e, exc_info=True)
            except Exception as e:
                logger.error('FEATURE EXTRACTION FAILED: %s' % e, exc_info=True)
            print(patient[entry],roi)
        else:
            print('%s is not exist in RTSTRUCTURE', roi)
      
 #----------------------------For all ROI-----------------

def executeORAWbatch_all(ptid,roi,myStructUID,exportDir,export_format,export_name,Img_path,RT_path,excludeStructRegex, includeStructRegex):
  outPath = r''
  logger = logging.getLogger('radiomics')

  # Set verbosity level for output to stderr (default level = WARNING)
  #radiomics.setVerbosity(logging.INFO)

  logger.info('pyradiomics version: %s', radiomics.__version__)
  logger.info('Reading Params file for pyradiomics')
  # Reading Params file for pyradiomics
  try:
    paramsFile = os.path.join(os.getcwd(),'ParamsSettings','Pyradiomics_Params.yaml')
  except Exception:
    logger.error('Could not find params file of Pyradiomics!', exc_info=True)
    exit(-1)
#  logger.info('Reading Params file of Pyrex')
  patient = ptid
#  logger.info('Parsing DICOM files and RTSTRUCT in working directory')
  #xnat_collection(myWorkingDirectory,collectionURL,myProjectID)
  #Img_path,RT_path = ParseStructure(myWorkingDirectory) #detect the path of Image and RTstruct
  logger.info('DICOM and RTSTRUCT Parsing Done')

  flists = pandas.DataFrame(data= {'patient':patient}).T # create a pandas data frame for data
  logger.info('Starting Pyrex')

  # define export output format
  if export_format == 'csv':
    RESULT = pandas.DataFrame()
  else:
    RESULT = Graph()

  for entry in flists:
      #results = pandas.DataFrame()
#      logger.info('processing patient: %s', patient[entry])
      mask_vol=PyrexReader.Read_RTSTRUCT(RT_path[entry])
      logger.info('Loading RTSTRUCT: %s', RT_path[entry])
      M=mask_vol[0]
      target = []
      for j in range(0,len(M.StructureSetROISequence)):
          target.append(M.StructureSetROISequence[j].ROIName)
      logger.info('ROI: %s', target)
      for k in range(0,len(target)):
          if re.search(excludeStructRegex,target[k]):
            if re.search(includeStructRegex, target[k]):
              print("ROI '%s' in blacklist, but also in whitelist. Start processing" % target[k])
            else:
              print('skip ROI: %s' % target[k])
              continue
          try:
              oraw_uid = pandas.Series({'uuid':uuid.uuid4().__str__()})
              featureVector = oraw_uid.append(flists[entry])
              start_time = time.clock()
              Image,Mask = PyrexReader.Img_Bimask(Img_path[entry],RT_path[entry],target[k])
              print("Binary Mask generation - time %s seconds ---" % (time.clock() - start_time))
              logger.info('Processing Radiomics on %s of Patient (%s)',target[k],patient[entry])
              if export_format == 'csv': # save results in csv
                  try:
                      
                      result = pandas.Series(PyrexWithParams.CalculationRun(Image,Mask,paramsFile))
                      contour = pandas.Series({'contour':target[k]})
                      structUID = pandas.Series({'structUID':myStructUID})
                      featureVector = featureVector.append(contour)
                      featureVector = featureVector.append(structUID)
                      featureVector = featureVector.append(result)
                      featureVector.name = k
                      results = pandas.DataFrame()
                      results = results.join(featureVector, how='outer')
                      Image = []
                      Mask= []
                      result=[]
                      # uid_frame = pandas.DataFrame()
                      # uid_frame = uid_frame.add(oraw_uid)
                      RESULT = pandas.concat([RESULT,results],axis=1)
                  except Exception as e:
                    logger.error('FEATURE EXTRACTION FAILED for CSV output: %s' % e, exc_info=True)
              else:# save results in triple store
                  try:
                    start_time = time.clock()
                    featureVector = PyrexWithParams.CalculationRun(Image,Mask,paramsFile) #compute radiomics
                    print("Pyradiomics Feature Extraction - time %s seconds ---" % (time.clock() - start_time))
                    print("featureVector: %s" % featureVector)
                    featureVector.update({'patient':patient[entry],'contour':target[k],'RTid':myStructUID}) #add patient ID and contour                
                    graph_roi = PyrexOutput.RadiomicsRDF(featureVector,exportDir,patient[entry],myStructUID,target[k],export_format,export_name) #store radiomics locally with a specific format 
                    RESULT = RESULT + graph_roi
                    logger.info('Extraction complete, writing rdf')
                  except Exception as e:
                    logger.error('FEATURE EXTRACTION FAILED for RDF output: %s' % e, exc_info=True)
          except Exception as e:
              logger.info('FEATURE EXTRACTION FAILED:: %s' % e, exc_info=True)
          logger.info('-------------------------------------------')
          print(patient[entry],target[k])
  return RESULT