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
import s3fs

script_dir = os.path.dirname(__file__)

rar_path = 'brookings-india/bescom-data'
extraction_path = '/home/ec2-user/temp_extraction_folder/'
csv_creation_path = '/home/ec2-user/csv_path/'
rar_copy_path = '/home/ec2-user/temp_rar_path/'
log_path = os.path.join(script_dir, 'bescom_scraper.log')

logging.basicConfig(filename=log_path, level=logging.INFO)


def single_thread_create_csvs(excel_file_path):
    for root, dirs, files in os.walk(excel_file_path):
        for file in files:
            if file.endswith('.xls') and '66' in file:
                print(os.path.join(root, file))
                filepath = os.path.join(root, file)
                station_regex = re.search('.*/(.*)_\d+_(\d+_\d+_\d+).xls', filepath)
                station_name = station_regex.group(1)
                date = station_regex.group(2)
                file_df = pd.ExcelFile(filepath)
                try:
                    write_66kv_csvs(file_df, station_name, date)
                except Exception as e:
                    print('Could not write 66kv file: ' + str(e))
                try:
                    write_transformer_csvs(file_df, station_name, date)
                except Exception as e:
                    print('Could not write transformer file: ' + str(e))
                try:
                    write_11kv_csvs(file_df, station_name, date)
                except Exception as e:
                    print('Could not write 11kv file: ' + str(e))
                return


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
        if file.endswith('.xls') and '66' in file:
            station_regex = re.search('.*/(.*)_\d+_(\d+_\d+_\d+).xls', filepath)
            station_name = station_regex.group(1)
            date = station_regex.group(2)
            excel_file = pd.ExcelFile(filepath)
            try:
                write_66kv_csvs(excel_file, station_name, date)
            except Exception as e:
                logging.error('Could not write 66kv file: ' + str(e))
            try:
                write_transformer_csvs(excel_file, station_name, date)
            except Exception as e:
                logging.error('Could not write transformer file: ' + str(e))
            try:
                write_11kv_csvs(excel_file, station_name, date)
            except Exception as e:
                logging.error('Could not write 11kv file: ' + str(e))
    except Exception as e:
        logging.exception(f'Error while writing csv for {str(root_and_file_tuple)}: {str(e)}')


def write_66kv_csvs(excel_file, station_name, date):
    file_df = excel_file.parse(skiprows=2, nrows=1442)

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    file_df.rename(columns={'Unnamed: 2': 'timestamp'}, inplace=True)
    file_df.drop(file_df.columns[[-1, -2]], axis=1, inplace=True)
    file_df.drop(0, axis=0, inplace=True)
    file_df.drop(file_df.filter(regex='Unnamed').columns, axis=1, inplace=True)
    file_df.drop(file_df.filter(regex='DUMMY').columns, axis=1, inplace=True)
    file_df.drop(file_df.filter(regex='11').columns, axis=1, inplace=True)

    # Get list of feeder names and their positions in the dataframe
    feeder_series = file_df.iloc[0].dropna()
    feeder_names_and_positions = list(
        zip([file_df.columns.get_loc(c) for c in feeder_series.index], feeder_series.tolist()))

    # Remove feeder row from dataframe
    full_substation_df = file_df.drop(1, axis=0)
    full_substation_df.replace(r'\s+', np.nan, regex=True, inplace=True)

    # Return if there are less than three columns
    if len(full_substation_df.columns) < 3:
        return

    # Ensure the first two columns are phvolt and stnbatvoltage
    if 'B_PHVOLT' in full_substation_df.columns[1] and 'STNBATVOLTAGE' in full_substation_df.columns[2]:
        substation_df = full_substation_df.iloc[:, 0:3]
        substation_df.columns = ['timestamp', 'phvolt', 'stnbatvoltage']
        substation_df['substation_name'] = station_name.lower()
        substation_df.to_csv(
            os.path.join(csv_creation_path, f'substation_66kv_lines/substation_{station_name}_66kv_line_{date}.csv.gz'),
            index=False, compression='gzip')

    for column, name in feeder_names_and_positions:
        regex_match = re.search('(F\d)-(.*)', name)
        if not regex_match:
            continue
        feeder_number = regex_match.group(1)
        feeder_name = regex_match.group(2)
        feeder_df = full_substation_df[full_substation_df.columns[column:column + 4]]
        feeder_df.columns = ['active_power', 'reactive_power', 'active_energy_imp', 'active_energy_exp']
        feeder_df['timestamp'] = full_substation_df['timestamp']
        feeder_df['substation_name'] = station_name.lower()
        feeder_df['feeder_name'] = feeder_name.lower()
        feeder_df.to_csv(os.path.join(csv_creation_path,
                                      f'substation_66kv_line_feeders/substation_{station_name}_66kv_line_feeder_{feeder_number}_{date}.csv.gz'),
                         index=False, compression='gzip')


def write_transformer_csvs(excel_file, station_name, date):
    file_df = excel_file.parse(skiprows=2002, nrows=1442)

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    file_df.rename(columns={'Unnamed: 2': 'timestamp'}, inplace=True)
    file_df.drop(0, axis=0, inplace=True)
    file_df.drop(file_df.filter(regex='Unnamed').columns, axis=1, inplace=True)
    file_df.drop(file_df.filter(regex='DUMMY').columns, axis=1, inplace=True)

    # Get list of transformer names and their positions in the dataframe
    transformer_series = file_df.iloc[0].dropna()
    transformer_names_and_positions = list(
        zip([file_df.columns.get_loc(c) for c in transformer_series.index], transformer_series.tolist()))

    # Remove transformer row from dataframe
    substation_transformer_df = file_df.drop(1, axis=0)
    substation_transformer_df.replace(r'\s+', np.nan, regex=True, inplace=True)

    # Return if there are less than two columns
    if len(substation_transformer_df.columns) < 2:
        return

    # Ensure the first two columns are phvolt and stnbatvoltage
    if 'TOTACTIVEPOWER' in substation_transformer_df.columns[1] and 'MVAR' in substation_transformer_df.columns[-1]:
        substation_df = substation_transformer_df.iloc[:, [0, 1, -1]]
        substation_df.columns = ['timestamp', 'total_active_power', 'station_mvar']
        substation_df['substation_name'] = station_name.lower()
        substation_df.to_csv(os.path.join(csv_creation_path,
                                          f'substation_transformers/substation_{station_name}_transformer_{date}.csv.gz'),
                             index=False, compression='gzip')

    for column, name in transformer_names_and_positions:
        regex_match = re.search('(T\d)-.*', name)
        if not regex_match:
            continue
        sub_transformer_name = regex_match.group(1)
        sub_transformer_df = substation_transformer_df[substation_transformer_df.columns[column:column + 4]]
        sub_transformer_df.columns = ['active_power', 'reactive_power', 'active_energy_imp', 'active_energy_exp']
        sub_transformer_df['timestamp'] = substation_transformer_df['timestamp']
        sub_transformer_df['substation_name'] = station_name.lower()
        sub_transformer_df['transformer_name'] = sub_transformer_name.lower()
        sub_transformer_df.to_csv(os.path.join(csv_creation_path,
                                               f'substation_transformer_sub_transformers/substation_{station_name}_transformer_sub_transformer_{sub_transformer_name}_{date}.csv.gz'),
                                  index=False, compression='gzip')


def write_11kv_csvs(excel_file, station_name, date):
    file_df = excel_file.parse(skiprows=4002, nrows=1442)

    # Clean up of the dataframe to remove dummy, blank and columns for 11 kv feeders
    file_df.rename(columns={'Unnamed: 2': 'timestamp'}, inplace=True)
    file_df.drop(0, axis=0, inplace=True)
    file_df.drop(file_df.filter(regex='Unnamed').columns, axis=1, inplace=True)
    file_df.drop(file_df.filter(regex='DUMMY').columns, axis=1, inplace=True)

    # Get list of feeder names and their positions in the dataframe
    feeder_series = file_df.iloc[0].dropna()
    feeder_names_and_positions = list(
        zip([file_df.columns.get_loc(c) for c in feeder_series.index], feeder_series.tolist()))

    # Remove feeder row from dataframe
    substation_11kv_df = file_df.drop(1, axis=0)
    substation_11kv_df.replace(r'\s+', np.nan, regex=True, inplace=True)

    # Return if there are less than two columns
    if len(substation_11kv_df.columns) < 2:
        return

    for column, name in feeder_names_and_positions:
        regex_match = re.search('(F\d)-(.*)', name)
        if not regex_match:
            continue
        feeder_number = regex_match.group(1)
        feeder_name = regex_match.group(2)
        feeder_df = substation_11kv_df[substation_11kv_df.columns[column:column + 7]]
        feeder_df.columns = ['aux_ib', 'aux_ir', 'aux_iy', 'active_power', 'reactive_power', 'active_energy_imp',
                             'active_energy_exp']
        feeder_df['timestamp'] = substation_11kv_df['timestamp']
        feeder_df['substation_name'] = station_name.lower()
        feeder_df['feeder_name'] = feeder_name.lower()
        feeder_df.to_csv(os.path.join(csv_creation_path,
                                      f'substation_11kv_line_feeders/substation_{station_name}_11kv_line_feeder_{feeder_number}_{date}.csv.gz'),
                         index=False, compression='gzip')


if __name__ == '__main__':
    start = time.time()
    try:
        s3 = s3fs.S3FileSystem(anon=False)
        os.makedirs(rar_copy_path, exist_ok=True)
        temp_rar_location = os.path.join(rar_copy_path, 'current_rar.rar')
        for file in s3.walk(rar_path):
            if file.endswith('.rar') and 'IPP' not in file and 'GEN' not in file:
                file_tags = s3.get_tags(file)
                if 'status' in file_tags and file_tags['status'] != 'failed':
                    logging.info(f'Skipping file {file} because it has status {file_tags["status"]}')
                    continue
                try:
                    os.makedirs(os.path.join(csv_creation_path, 'substation_66kv_lines'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_66kv_line_feeders'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_transformers'), exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_transformer_sub_transformers'),
                                exist_ok=True)
                    os.makedirs(os.path.join(csv_creation_path, 'substation_11kv_line_feeders'), exist_ok=True)
                    os.makedirs(extraction_path, exist_ok=True)


                    s3.put_tags(file, {'status': 'processing'})
                    logging.info(f'Working on file {file}')
                    s3.get(file, temp_rar_location)
                    patoolib.extract_archive(temp_rar_location, outdir=extraction_path)
                    os.remove(temp_rar_location)
                    # single_thread_create_csvs(extraction_path)
                    create_csvs(extraction_path)
                    os.system(f'aws s3 cp --recursive {csv_creation_path} s3://brookings-india/bescom-data/converted_csvs')
                    s3.put_tags(file, {'status': 'processed'})
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
        os.system(f'aws s3 cp {log_path} s3://brookings-india/bescom-data/logs/{start}/bescom_converter.log')
        os.system('sudo shutdown -h now')
