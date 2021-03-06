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
import s3fs

script_dir = os.path.dirname(__file__)

extraction_path = '/home/ec2-user/temp_extraction_folder/'
csv_creation_path = '/home/ec2-user/csv_path/'
rar_copy_path = '/home/ec2-user/temp_rar_path/'
log_path = os.path.join(script_dir, 'bescom_converter.log')

in_feeder_whitelisted_columns = ['ACTIVEPOWER', 'REACTIVEPOWER', 'ACTIVEENERGYEXP', 'ACTIVEENERGYIMP']
transformer_line_whitelisted_columns = ['ACTIVEPOWER', 'REACTIVEPOWER', 'ACTIVEENERGYEXP', 'ACTIVEENERGYIMP']
out_feeder_whitelisted_columns = ['ACTIVEPOWER', 'REACTIVEPOWER', 'ACTIVEENERGYEXP',
                                  'ACTIVEENERGYIMP', 'IB', 'IR', 'IY']

logging.basicConfig(filename=log_path, level=logging.INFO)


def create_csvs(excel_file_path):
    with multiprocessing.Pool(36) as pool:
        walk = os.walk(excel_file_path)
        fn_gen = itertools.chain.from_iterable(((root, file) for file in files) for root, dirs, files in walk)
        pool.map(write_csvs_for_file, fn_gen)


def write_csvs_for_file(root_and_file_tuple):
    try:
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
    except Exception as e:
        logging.exception(f'FAILED FILE Error while writing csv for {str(root_and_file_tuple)}: {str(e)}')


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
        column_regex = re.search('.*?(\d+)(.*?)((RE)?ACTIVE.*)', column.strip().replace(' ', ''))
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

    feeder_dict = handle_feeders_w_re_ending(feeder_dict)

    for feeder_name in feeder_dict:
        feeder_voltage = feeder_voltage_dict.get(feeder_name, feeder_voltage_dict.get(feeder_name[:-2], 'NA'))
        feeder_df = pd.DataFrame.from_dict(feeder_dict[feeder_name])[in_feeder_whitelisted_columns]
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
        column_regex = re.search('.*?(\d+)(LV\d+)(.*)', column.strip().replace(' ', ''))
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
        line_voltage = low_voltage_lines_voltage_dict.get(line_name,
                                                          low_voltage_lines_voltage_dict.get(line_name[:-2], 'NA'))
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
        column_regex = re.search('.*?(\d+)(.*?)(R?E?ACTIVE.*)', column.strip().replace(' ', ''))
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

    handle_feeders_w_re_ending(lines_dict)

    for line_name in lines_dict:
        line_voltage = lines_voltage_dict.get(line_name, lines_voltage_dict.get(line_name[:-2], 'NA'))
        line_df = pd.DataFrame.from_dict(lines_dict[line_name])[transformer_line_whitelisted_columns]
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
    feeder_names_part_1 = get_feeder_name_series(block_df_part_1)
    block_df_part_1 = clean_block(block_df_part_1, [0, 1, 3, 4])

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    feeder_names_part_2 = get_feeder_name_series(block_df_part_2)
    block_df_part_2 = clean_block(block_df_part_2, [0, 1, 3, 4])

    block_df = pd.merge(block_df_part_1, block_df_part_2, on='timestamp', how='left')
    feeder_name_series = pd.concat([feeder_names_part_1, feeder_names_part_2], sort=False)
    timestamp_column = block_df['timestamp'].values

    # Write nothing if there are less than two columns
    if len(block_df.columns) < 2:
        logging.info(f'{substation_name}_{voltage}_{date} has no data for block 3')
        return

    out_feeders_df = block_df.iloc[:, 1:]

    write_out_feeders_csvs(out_feeders_df, substation_name, date, timestamp_column, voltage, feeder_name_series)


def write_out_feeders_csvs(out_feeders_df, substation_name, date, timestamp_column, substation_voltage,
                           feeder_name_series):
    feeder_dict = {}
    feeder_voltage_dict = {}
    feeder_names_dict = {}

    for column in out_feeders_df:
        column_regex = re.search('.*?(\d+)(.*?)(I[A-Z]$|((RE)?ACTIVE.*)$)', column.strip().replace(' ', ''))
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
        if column_type == 'REACTIVEPOWER' or column_type == 'IY':
            try:
                feeder_names_dict[feeder_name] = feeder_name_series.get(column)
            except Exception as e:
                logging.error(f'FEEDER NAME missing for {substation_name}_{substation_voltage}_{date}_{feeder_name}'
                              f'_{column_type}')

    feeder_dict = handle_feeders_w_re_ending(feeder_dict)


    for feeder_name in feeder_dict:
        feeder_voltage = feeder_voltage_dict.get(feeder_name, feeder_voltage_dict.get(feeder_name[:-2], 'NA'))
        feeder_df = pd.DataFrame.from_dict(feeder_dict[feeder_name])[out_feeder_whitelisted_columns]
        feeder_df = feeder_df.assign(timestamp=timestamp_column)
        feeder_df = feeder_df.assign(substation_name=substation_name.lower())
        feeder_df = feeder_df.assign(feeder_name=feeder_name.lower())
        feeder_df = feeder_df.assign(feeder_full_name=feeder_names_dict.get(feeder_name, 'NA'))
        if feeder_name not in feeder_names_dict:
            logging.error(f'FEEDER NAME PROBLEM FOR {substation_name}_{substation_voltage}_{date}_{feeder_name}')
        feeder_df.to_csv(os.path.join(csv_creation_path,
                                      f'substation_out_feeders/{substation_name}_{substation_voltage}_out_feeder_'
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
                                                    'ACTIVEENERGYEXP': np.nan, 'ACTIVEENERGYIMP': np.nan,
                                                    'IB': np.nan, 'IR': np.nan, 'IY': np.nan}
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


def clean_block(block_df, columns_to_drop):
    block_df.rename(columns={block_df.columns[2]: 'timestamp'}, inplace=True)
    block_df.drop([0, 1], axis=0, inplace=True)
    block_df.drop(block_df.columns[columns_to_drop], axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='Unnamed').columns, axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='DUMMY').columns, axis=1, inplace=True)
    block_df.drop(block_df.filter(regex='^\d\.?\d*$').columns, axis=1, inplace=True)
    block_df.replace(r'\s+', np.nan, regex=True, inplace=True)
    return block_df


def get_feeder_name_series(block_df):
    return block_df.iloc[1, :].fillna(method='ffill')

if __name__ == '__main__':
    arguments = sys.argv
    rar_path = arguments[1]
    file_regex = arguments[2]
    start = time.time()
    try:
        s3 = s3fs.S3FileSystem(anon=False)
        os.makedirs(rar_copy_path, exist_ok=True)
        temp_rar_location = os.path.join(rar_copy_path, 'current_rar.rar')
        for file in s3.walk(rar_path):
            if file.endswith('.rar') and 'IPP' not in file and 'GEN' not in file and file_regex in file:
                file_tags = s3.get_tags(file)
                if 'status' in file_tags and file_tags['status'] in ['processed_v2', 'processing_v2']:
                    logging.info(f'Skipping file {file} because it has status {file_tags["status"]}')
                    continue
                try:
                    os.makedirs(os.path.join(csv_creation_path, 'substation_lines'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_in_feeders'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_transformers'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'transformer_lines'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_out_feeders'), exist_ok=True)
                    os.makedirs(extraction_path, exist_ok=True)


                    s3.put_tags(file, {'status': 'processing_v2'})
                    logging.info(f'Working on file {file}')
                    s3.get(file, temp_rar_location)
                    patoolib.extract_archive(temp_rar_location, outdir=extraction_path)
                    os.remove(temp_rar_location)
                    create_csvs(extraction_path)
                    os.system(f'aws s3 cp --recursive {csv_creation_path} s3://brookings-india-bescom-data'
                              f'/converted_csvs')
                    os.system(f'aws s3 cp {log_path} s3://brookings-india-bescom-data/logs/{rar_path}_{file_regex}/'
                              f'{start}/bescom_converter.log')
                    s3.put_tags(file, {'status': 'processed_v2'})
                except Exception as e:
                    logging.exception(f'Error while handling file {file}: {str(e)}')
                    s3.put_tags(file, {'status': 'failed'})
                finally:
                    try:
                        shutil.rmtree(extraction_path)
                        shutil.rmtree(csv_creation_path)
                        if os.path.exists(temp_rar_location):
                            os.remove(temp_rar_location)
                    except Exception as e:
                        logging.exception(f'Could not clean up files for file {file}!')
    except Exception as e:
        logging.exception(f'Script failed with error {str(e)}!')
    finally:
        end = time.time()
        logging.info(f'Script took {(end - start)} seconds to run')
        os.system(f'aws s3 cp {log_path} s3://brookings-india-bescom-data/logs/{rar_path}_{file_regex}/'
                  f'{start}/bescom_converter.log')
        os.system('sudo shutdown -h now')
