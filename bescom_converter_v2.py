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
import rarfile
import sys

start = time.time()

script_dir = os.path.dirname(__file__)

# rar_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/02.FEB/'
extraction_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/temp_extraction_folder/'
csv_creation_path = '/Users/utkarshdalal/Documents/Brookings Karnataka Data/csv_path/'

os.makedirs(os.path.join(csv_creation_path, 'substation_lines'), exist_ok=True)
os.makedirs(os.path.join(csv_creation_path, 'substation_in_feeders'), exist_ok=True)
os.makedirs(os.path.join(csv_creation_path, 'substation_transformers'), exist_ok=True)
os.makedirs(os.path.join(csv_creation_path, 'transformer_lines'), exist_ok=True)
os.makedirs(os.path.join(csv_creation_path, 'substation_out_feeders'), exist_ok=True)

logging.basicConfig(filename=os.path.join(script_dir, 'bescom_converter.log'), level=logging.INFO)


def create_csvs(excel_file_path):
    with multiprocessing.Pool(4) as pool:
        walk = os.walk(excel_file_path)
        fn_gen = itertools.chain.from_iterable(((root, file) for file in files) for root, dirs, files in walk)
        pool.map(write_csvs_for_file, fn_gen)


def write_csvs_for_file(root_and_file_tuple):
    root = root_and_file_tuple[0]
    file = root_and_file_tuple[1]
    filepath = os.path.join(root, file)
    logging.info(f'Started working on {filepath}')
    if file.endswith('.xls'):
        station_regex = re.search('.*/(.*)_(\d+)_(\d+_\d+_\d+).xls', filepath)
        station_name = station_regex.group(1)
        voltage = station_regex.group(2)
        date = station_regex.group(3)
        excel_file = pd.ExcelFile(filepath)
        try:
            write_csvs_for_block_1(excel_file, station_name, date, voltage)
        except Exception as e:
            logging.exception(f'Could not write block 1 files for file {file}: ' + str(e))
            logging.error(f'FAILED FILE {file}')
        try:
            write_csvs_for_block_2(excel_file, station_name, date, voltage)
        except Exception as e:
            logging.exception(f'Could not write block 2 files for file {file}: ' + str(e))
            logging.error(f'FAILED FILE {file}')
        try:
            write_csvs_for_block_3(excel_file, station_name, date, voltage)
        except Exception as e:
            logging.exception(f'Could not write block 3 files for file {file}: ' + str(e))
            logging.error(f'FAILED FILE {file}')


def write_csvs_for_block_1(excel_file, substation_name, date, voltage):
    block_df = excel_file.parse(skiprows=2, nrows=1442)

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    block_df = clean_block(block_df, [0, 1, 3, 4, -1, -2])

    # Write nothing if there are less than three columns
    if len(block_df.columns) < 3:
        logging.info(f'{substation_name}_{voltage}_{date} has no data for block 1')
        return

    timestamp_column = block_df['timestamp'].values
    substation_df = block_df.iloc[:, 0:3]
    write_substation_csv(substation_df, substation_name, date, voltage)

    for column in block_df.iloc[:, 3:].columns:
        if re.match('.*\d{2}(L|H)V\d.*', column):
            column_index = block_df.columns.get_loc(column)
            block_df = block_df.iloc[:, :column_index]
            break
    in_feeders_df = block_df.filter(regex=f'.*R?E?ACTIVE.*')
    write_in_feeders_csvs(in_feeders_df, substation_name, date, timestamp_column, voltage)

#     lv_lines_df = block_df.filter(regex='.*\d{2}LV\d.*')
#     write_lv_lines_csvs(lv_lines_df, substation_name, date, timestamp_column, voltage)


def write_substation_csv(substation_df, substation_name, date, voltage):
    if 'PHVOLT' in substation_df.columns[1] and 'STNBATVOLTAGE' in substation_df.columns[2]:
        substation_df.columns = ['timestamp', 'phvolt', 'stnbatvoltage']
        substation_df = substation_df.assign(substation_name=substation_name.lower())
        substation_df.to_csv(
            os.path.join(csv_creation_path,
                         f'substation_lines/{substation_name}_{voltage}_kv_line_{date}.csv.gz'),
            index=False, compression='gzip')


def write_in_feeders_csvs(in_feeders_df, substation_name, date, timestamp_column, voltage):
    feeder_dict = {}
    feeder_voltage_dict = {}

    for column in in_feeders_df:
        column_regex = re.search('.*?(\d+)(.*?)((RE)?ACTIVE.*)', column.strip())
        if column_regex is None:
            continue
        voltage = column_regex.group(1)
        feeder_name = column_regex.group(2).replace('/', '')
        column_type = column_regex.group(3)
        if feeder_name not in feeder_dict:
            feeder_dict[feeder_name] = {'ACTIVEPOWER': np.nan, 'REACTIVEPOWER': np.nan,
                                        'ACTIVEENERGYEXP': np.nan, 'ACTIVEENERGYIMP': np.nan}
        feeder_dict[feeder_name][column_type] = in_feeders_df[column].values
        feeder_voltage_dict[feeder_name] = voltage

    for feeder_name in feeder_dict:
        feeder_voltage = feeder_voltage_dict[feeder_name]
        feeder_df = pd.DataFrame.from_dict(feeder_dict[feeder_name])
        feeder_df = feeder_df.assign(timestamp=timestamp_column)
        feeder_df = feeder_df.assign(substation_name=substation_name.lower())
        feeder_df = feeder_df.assign(feeder_name=feeder_name.lower())
        feeder_df.to_csv(os.path.join(csv_creation_path,
                                      f'substation_in_feeders/{substation_name}_{voltage}_in_feeder_{feeder_voltage}'
                                      f'_kv_{feeder_name}_{date}.csv.gz'),
                         index=False, compression='gzip')


def write_lv_lines_csvs(lv_lines_df, substation_name, date, timestamp_column, voltage):
    low_voltage_lines_dict = {}
    low_voltage_lines_voltage_dict = {}

    for column in lv_lines_df:
        column_regex = re.search('.*?(\d+)(LV\d+)(.*)', column.strip())
        if column_regex is None:
            continue
        voltage = column_regex.group(1)
        line_name = column_regex.group(2).replace('/', '')
        column_type = column_regex.group(3)
        if line_name not in low_voltage_lines_dict:
            low_voltage_lines_dict[line_name] = {'Y-B_PHVOLT': np.nan, 'IB': np.nan, 'IR': np.nan, 'IY': np.nan}
        low_voltage_lines_dict[line_name][column_type] = lv_lines_df[column].values
        low_voltage_lines_voltage_dict[line_name] = voltage

    for line_name in low_voltage_lines_dict:
        line_voltage = low_voltage_lines_voltage_dict[line_name]
        line_df = pd.DataFrame.from_dict(low_voltage_lines_dict[line_name])
        line_df = line_df.assign(timestamp=timestamp_column)
        line_df = line_df.assign(substation_name=substation_name)
        line_df = line_df.assign(line_name=line_name)
        line_df.to_csv(os.path.join(csv_creation_path,
                                    f'substation_lv_lines/{substation_name}_{voltage}_lv_line_{line_voltage}'
                                    f'_kv_{line_name}_{date}.csv.gz'),
                       index=False, compression='gzip')


def write_csvs_for_block_2(excel_file, substation_name, date, voltage):
    block_df = excel_file.parse(skiprows=2002, nrows=1442)

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    block_df = clean_block(block_df, [0, 1, 3, 4])

    if len(block_df.columns) < 2:
        logging.info(f'{substation_name}_{voltage}_{date} has no data for block 2')
        return

    transformer_df = block_df.iloc[:, [0, 1]]
    station_mvar = np.nan
    if 'MVAR' in block_df.columns[-1]:
        station_mvar = block_df[block_df.columns[-1]].values
    transformer_df = transformer_df.assign(station_mvar=station_mvar)
    write_transformer_csv(transformer_df, substation_name, date, voltage)

    timestamp_column = block_df['timestamp'].values
    transformer_lines_df = block_df.iloc[:, 2:].filter(regex='.*R?E?ACTIVE.*')
    write_transformer_lines_csvs(transformer_lines_df, substation_name, date, timestamp_column, voltage)


def write_transformer_csv(transformer_df, substation_name, date, voltage):
    if 'TOTACTIVEPOWER' in transformer_df.columns[1]:
        transformer_df.columns = ['timestamp', 'total_active_power', 'station_mvar']
        transformer_df['substation_name'] = substation_name.lower()
        transformer_df.to_csv(os.path.join(csv_creation_path,
                                           f'substation_transformers/substation_{substation_name}_{voltage}'
                                           f'_transformer_{date}.csv.gz'),
                              index=False, compression='gzip')


def write_transformer_lines_csvs(transformer_lines_df, substation_name, date, timestamp_column, voltage):
    lines_dict = {}
    lines_voltage_dict = {}

    for column in transformer_lines_df:
        column_regex = re.search('.*?(\d+)(.*?)(R?E?ACTIVE.*)', column.strip())
        if column_regex is None:
            continue
        voltage = column_regex.group(1)
        line_name = column_regex.group(2).replace('/', '')
        column_type = column_regex.group(3)
        if line_name not in lines_dict:
            lines_dict[line_name] = {'ACTIVEPOWER': np.nan, 'REACTIVEPOWER': np.nan,
                                     'ACTIVEENERGYEXP': np.nan, 'ACTIVEENERGYIMP': np.nan}
        lines_dict[line_name][column_type] = transformer_lines_df[column].values
        lines_voltage_dict[line_name] = voltage

    for line_name in lines_dict:
        line_voltage = lines_voltage_dict[line_name]
        line_df = pd.DataFrame.from_dict(lines_dict[line_name])
        line_df = line_df.assign(timestamp=timestamp_column)
        line_df = line_df.assign(substation_name=substation_name)
        line_df = line_df.assign(line_name=line_name)
        line_df.to_csv(os.path.join(csv_creation_path,
                                    f'transformer_lines/{substation_name}_{voltage}_transformer_line_{line_voltage}'
                                    f'_kv_{line_name}_{date}.csv.gz'),
                       index=False, compression='gzip')


def write_csvs_for_block_3(excel_file, substation_name, date, voltage):
    block_df_part_1 = excel_file.parse(skiprows=4002, nrows=1442)
    block_df_part_2 = excel_file.parse(skiprows=6002, nrows=1442)

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    block_df_part_1 = clean_block(block_df_part_1, [0, 1, 3, 4])

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    block_df_part_2 = clean_block(block_df_part_2, [0, 1, 3, 4])

    block_df = pd.merge(block_df_part_1, block_df_part_2, on='timestamp', how='left')
    timestamp_column = block_df['timestamp'].values

    # Write nothing if there are less than two columns
    if len(block_df.columns) < 2:
        logging.info(f'{substation_name}_{voltage}_{date} has no data for block 3')
        return

    out_feeders_df = block_df.iloc[:, 1:]

    write_out_feeders_csvs(out_feeders_df, substation_name, date, timestamp_column, voltage)


def write_out_feeders_csvs(out_feeders_df, substation_name, date, timestamp_column, substation_voltage):
    feeder_dict = {}
    feeder_voltage_dict = {}

    for column in out_feeders_df:
        column_regex = re.search('.*?(\d+)(.*?)(I[A-Z]$|((RE)?ACTIVE.*)$)', column.strip())
        if column_regex is None:
            continue
        voltage = column_regex.group(1)
        feeder_name = column_regex.group(2).replace('/', '')
        column_type = column_regex.group(3)
        if feeder_name not in feeder_dict:
            feeder_dict[feeder_name] = {'ACTIVEPOWER': np.nan, 'REACTIVEPOWER': np.nan,
                                        'ACTIVEENERGYEXP': np.nan, 'ACTIVEENERGYIMP': np.nan,
                                        'IB': np.nan, 'IR': np.nan, 'IY': np.nan}
        feeder_dict[feeder_name][column_type] = out_feeders_df[column].values
        feeder_voltage_dict[feeder_name] = voltage

    for feeder_name in feeder_dict:
        feeder_voltage = feeder_voltage_dict[feeder_name]
        feeder_df = pd.DataFrame.from_dict(feeder_dict[feeder_name])
        feeder_df = feeder_df.assign(timestamp=timestamp_column)
        feeder_df = feeder_df.assign(substation_name=substation_name.lower())
        feeder_df = feeder_df.assign(feeder_name=feeder_name.lower())
        feeder_df.to_csv(os.path.join(csv_creation_path,
                                      f'substation_out_feeders/{substation_name}_{substation_voltage}_out_feeder_'
                                      f'{feeder_voltage}_kv_{feeder_name}_{date}.csv.gz'),
                         index=False, compression='gzip')


def clean_block(block_df, columns_to_drop):
    block_df.rename(columns={block_df.columns[2]: 'timestamp'}, inplace=True)
    block_df.drop([0, 1], axis=0, inplace=True)
    block_df.drop(block_df.columns[columns_to_drop], axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='Unnamed').columns, axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='DUMMY').columns, axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='^\d\.?\d*$').columns, axis=1, inplace=True)
    block_df.replace(r'\s+', np.nan, regex=True, inplace=True)
    return block_df


if __name__ == '__main__':
    arguments = sys.argv
    rar_path = arguments[1]
    file_regex = arguments[2]
    print(rar_path)
    print(file_regex)
    start = time.time()
    try:
        for root, dirs, files in os.walk(rar_path):
            for file in files:
                if file.endswith('.rar') and 'IPP' not in file and 'GEN' not in file and file_regex in file:
                    temp_path = os.path.join(extraction_path, 'temp')
                    os.makedirs(temp_path, exist_ok=True)
                    try:
                        file_path = os.path.join(root, file)
                        print(os.path.join(root, file))
                        # rf = rarfile.RarFile(file_path)
                        # rf.extract_all(path=temp_path)
                        patoolib.extract_archive(file_path, outdir=temp_path)
                        create_csvs(temp_path)
                    finally:
                        shutil.rmtree(temp_path)
    finally:
        end = time.time()
        logging.info(f'Script took {(end - start)} seconds to run')
