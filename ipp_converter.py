import os
import pandas as pd
import numpy as np
import re
import patoolib
import shutil
import time
import itertools
import multiprocessing
import logging
import sys

script_dir = os.path.dirname(__file__)

# rar_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/02.FEB/'
extraction_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/temp_extraction_folder/'
csv_creation_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/redshift_csv_path/'

whitelisted_columns = ['ACTIVEPOWER', 'REACTIVEPOWER', 'ACTIVEENERGYEXP', 'ACTIVEENERGYIMP']

os.makedirs(os.path.join(csv_creation_path, 'ipp_feeders'), exist_ok=True)

logging.basicConfig(filename=os.path.join(script_dir, 'ipp_converter.log'), level=logging.INFO)


def create_csvs(excel_file_path):
    with multiprocessing.Pool(4) as pool:
        walk = os.walk(excel_file_path)
        fn_gen = itertools.chain.from_iterable(((root, file) for file in files) for root, dirs, files in walk)
        pool.map(write_csvs_for_file, fn_gen)


def write_csvs_for_file(root_and_file_tuple):
    try:
        root = root_and_file_tuple[0]
        file = root_and_file_tuple[1]
        filepath = os.path.join(root, file)
        logging.info(f'Started working on {filepath}')
        if 'IPP' in file and file.endswith('.xls'):
            station_regex = re.search('.*/IPP_(.*)_(\d+_\d+_\d+).xls', filepath)
            station_name = station_regex.group(1)
            date = station_regex.group(2)
            excel_file = pd.ExcelFile(filepath)
            try:
                write_csvs(excel_file, station_name, date)
            except Exception as e:
                logging.exception(f'Could not write block data for file {file}: ' + str(e))
                logging.error(f'FAILED FILE {file}')
    except Exception as e:
        logging.exception(f'FAILED FILE Error while writing csv for {str(root_and_file_tuple)}: {str(e)}')


def write_csvs(excel_file, substation_name, date):
    block_top = 0
    name_frame = excel_file.parse(skiprows=block_top, nrows=1)
    name_option1 = name_frame.columns[0]
    block_name = name_option1
    block_df = excel_file.parse(skiprows=block_top + 2, nrows=1442)
    while(not block_df.empty):
        block_df = clean_block(block_df, [0, 1, 3, 4])
        timestamp_column = block_df['timestamp'].values

        # Write nothing if there are less than two columns
        if len(block_df.columns) < 2:
            logging.info(f'{substation_name}_{block_name}_{date} has no data for block 3')
            return

        feeders_df = block_df.iloc[:, 1:].filter(regex='ACTIVE')

        write_block_csvs(feeders_df, substation_name, block_name, date, timestamp_column)

        block_top += 2000
        adjustment = 0
        name_frame = excel_file.parse(skiprows=block_top, nrows=1)
        if name_frame.empty:
            break
        name_frame2 = excel_file.parse(skiprows=block_top + 1, nrows=1)
        name_option1 = name_frame.columns[0]
        name_option2 = name_frame2.columns[0]
        block_name = name_option1
        if not (isinstance(name_option2, int) or 'Unnamed' in name_option2):
            block_name = name_option2
            adjustment = 1
        if 'Unnamed' in block_name:
            block_name = 'UNKNOWN'
            adjustment = 1
        block_df = excel_file.parse(skiprows=block_top + adjustment + 2, nrows=1442)


def clean_block(block_df, columns_to_drop):
    block_df.rename(columns={block_df.columns[2]: 'timestamp'}, inplace=True)
    block_df.drop([0, 1], axis=0, inplace=True)
    block_df.drop(block_df.columns[columns_to_drop], axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='Unnamed').columns, axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='DUMMY').columns, axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='^\d\.?\d*$').columns, axis=1, inplace=True)
    block_df.replace(r'\s+', np.nan, regex=True, inplace=True)
    return block_df


def write_block_csvs(feeders_df, substation_name, block_name, date, timestamp_column):
    feeder_dict = {}
    feeder_voltage_dict = {}

    for column in feeders_df:
        column_regex = re.search('(.*?)(\d+)(.*?)((RE)?ACTIVE.*)$', column.strip().replace(' ', ''))
        if column_regex is None:
            continue
        ipp_name = column_regex.group(1)
        voltage = column_regex.group(2)
        feeder_name = column_regex.group(3).replace('/', '')
        column_type = column_regex.group(4)
        if feeder_name not in feeder_dict:
            feeder_dict[feeder_name] = {'ACTIVEPOWER': np.nan, 'REACTIVEPOWER': np.nan,
                                        'ACTIVEENERGYEXP': np.nan, 'ACTIVEENERGYIMP': np.nan}
        feeder_dict[feeder_name][column_type] = feeders_df[column].values
        feeder_voltage_dict[feeder_name] = voltage

    feeder_dict = handle_feeders_w_re_ending(feeder_dict)


    for feeder_name in feeder_dict:
        feeder_voltage = feeder_voltage_dict.get(feeder_name, feeder_voltage_dict.get(feeder_name[:-2], 'NA'))
        feeder_df = pd.DataFrame.from_dict(feeder_dict[feeder_name])[whitelisted_columns]
        feeder_df = feeder_df.assign(timestamp=timestamp_column)
        feeder_df = feeder_df.assign(substation_name=substation_name.lower())
        feeder_df = feeder_df.assign(ipp_name=ipp_name.lower())
        feeder_df = feeder_df.assign(feeder_name=feeder_name.lower())
        feeder_df.to_csv(os.path.join(csv_creation_path,
                                      f'ipp_feeders/{substation_name}_{block_name}_{ipp_name}_feeder_'
                                      f'{feeder_voltage}_kv_{feeder_name}_{date}.csv.gz'),
                         index=False, compression='gzip')


def handle_feeders_w_re_ending(feeder_dict):
    keys_to_delete = []
    dict_to_add = {}
    for feeder_name in feeder_dict:
        current_feeder_dict = feeder_dict[feeder_name]
        if 'REACTIVEENERGYEXP' in current_feeder_dict.keys() or 'REACTIVEENERGYIMP' in current_feeder_dict.keys():
            correct_feeder_name = f'{feeder_name}RE'
            if correct_feeder_name not in feeder_dict:
                dict_to_add[correct_feeder_name] = {'ACTIVEPOWER': np.nan, 'REACTIVEPOWER': np.nan,
                                                    'ACTIVEENERGYEXP': np.nan, 'ACTIVEENERGYIMP': np.nan}
                dict_to_add[correct_feeder_name]['ACTIVEPOWER'] = current_feeder_dict.get('REACTIVEPOWER', np.nan)
                dict_to_add[correct_feeder_name]['ACTIVEENERGYEXP'] = current_feeder_dict.get('REACTIVEENERGYEXP',
                                                                                              np.nan)
                dict_to_add[correct_feeder_name]['ACTIVEENERGYIMP'] = current_feeder_dict.get('REACTIVEENERGYIMP',
                                                                                              np.nan)
            else:
                feeder_dict[correct_feeder_name]['ACTIVEPOWER'] = current_feeder_dict.get('REACTIVEPOWER', np.nan)
                feeder_dict[correct_feeder_name]['ACTIVEENERGYEXP'] = current_feeder_dict.get('REACTIVEENERGYEXP', np.nan)
                feeder_dict[correct_feeder_name]['ACTIVEENERGYIMP'] = current_feeder_dict.get('REACTIVEENERGYIMP', np.nan)
            keys_to_delete.append(feeder_name)
    for key in keys_to_delete:
        del feeder_dict[key]
    feeder_dict.update(dict_to_add)
    return feeder_dict


if __name__ == '__main__':
    arguments = sys.argv
    rar_path = arguments[1]
    file_regex = arguments[2]
    print(rar_path)
    print(file_regex)
    start = time.time()
    try:
        temp_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/02.FEB/IPP/IPP'
        create_csvs(temp_path)
        # patoolib.extract_archive(file_path, outdir=temp_path)
        # for root, dirs, files in os.walk(rar_path):
        #     for file in files:
        #         if file.endswith('.rar') and 'IPP' not in file and 'GEN' not in file and file_regex in file:
        #             temp_path = os.path.join(extraction_path, 'temp')
        #             os.makedirs(temp_path, exist_ok=True)
        #             try:
        #                 file_path = os.path.join(root, file)
        #                 print(os.path.join(root, file))
        #                 # rf = rarfile.RarFile(file_path)
        #                 # rf.extract_all(path=temp_path)
        #                 patoolib.extract_archive(file_path, outdir=temp_path)
        #                 create_csvs(temp_path)
        #             finally:
        #                 shutil.rmtree(temp_path)
    finally:
        end = time.time()
        logging.info(f'Script took {(end - start)} seconds to run')
