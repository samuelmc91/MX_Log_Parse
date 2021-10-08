import os
import sys
import pandas as pd
import matplotlib.pyplot as plt
import importlib
import warnings
import numpy as np
import datetime
import time

############################################################ Global Keys ############################################################
mount_key = 'executing:Mount'
unmount_key = 'executing:Unmount'
load_key = 'executing:Load'
sensor_key = 'executing:UC_Proximity'
key_dict = {mount_key: 'Mount', unmount_key: 'Unmount',
            load_key: 'Load', sensor_key: 'Sensors'}
############################################################ Methods ############################################################
def dir_check(new_dir_path):
    if not os.path.exists(new_dir_path):
        os.system('mkdir -p {}'.format(new_dir_path))
        return False
    else:
        return True

def csv_check(new_csv_path):
    if not os.path.exists(new_csv_path):
        base_df = pd.DataFrame(columns=['Date', 'Mounts'])
        base_df.to_csv(new_csv_path)

def create_output_dirs(output_dir_beam):
    output_sub_dirs = ['m_vision', 'sensors', 'sample_counter']
    output_sub_dirs_out = []
    for sub_dir in output_sub_dirs:
        new_dir_path = os.path.join(output_dir_beam, sub_dir)
        output_sub_dirs_out.append(new_dir_path)
        dir_check(new_dir_path)
    return output_sub_dirs_out

def make_csv_and_graph(df_list, column_title, output_dir, output_title):
    # print(df_list)
    log_df = pd.DataFrame(df_list, columns=["Date", "Time", column_title])
    log_df.to_csv(os.path.join(output_dir, output_title + '.csv'))
    if int(len(log_df.index)) <= 10:
        print('\tNot enough rows; # of rows: ', str(len(log_df.index)), '\n')
        return False
    tmp_y = pd.to_numeric(log_df[column_title]).values
    tmp_x = log_df['Time'].values

    n_ticks = len(log_df.index) // 10
    
    tmp_fig, tmp_axs = plt.subplots(1)
    tmp_axs.plot(tmp_x, tmp_y)

    tmp_axs.set_title(output_title)
    tmp_axs.set_xticks(tmp_axs.get_xticks()[::n_ticks])
    tmp_axs.tick_params(axis='x', rotation=-45)
    tmp_fig.savefig(os.path.join(output_dir, output_title + '.png'))
    plt.close()

def extract_info(log_line_in):
    log_line_time = log_line_in.split(';')[4].split(': ')[-1].strip()
    try:
        log_line_time = float(log_line_time)
        log_line_output = [log_line_in.split(
            ';')[0]] + [log_line_in.split(';')[1].split('.')[0]] + [log_line_time]
        return log_line_output
    except Exception as e:
        pass

def parse_logs(log_file_dir, log_pr_dir, m_vision_dir_in, sensor_dir_in, master_count_csv_in, sample_dir_in, lix_run=False):
    for log in log_file_dir:
        df_list_dict = {'Unmount': [], 'Mount': 0, 'Load': [], 'Sensors': {
            'Proximity Sensor 1': [], 'Proximity Sensor 2': [], 'Proximity Sensor 3': []}}

        log_open = open(log, 'r', errors='ignore')

        log_title = log.split('/')[-1].split('.')[0]
        log_lines = log_open.readlines()
        log_index = 0
        end_time = datetime.time(0,0,0,0)
        while log_index < (len(log_lines) - 1):
            # Try to catch possible mis-formatted lines that do not include the -2 index
            try:
                line_to_check = log_lines[log_index].split(';')[-2].replace(' ', '')
                my_time = log_lines[log_index].split(';')[1]
            except Exception as e:
                log_index += 1
                continue
            if not lix_run:
                if line_to_check in key_dict and line_to_check != 'executing:Mount':
                    log_index += 1
                    try:
                        extracted_info = extract_info(log_lines[log_index])
                    except Exception:
                        runtime_log_output.write('\nKeyword for {} at end of log: {}\n'.format(key_dict[line_to_check], log_title))
                        continue
                    
                    if extracted_info:
                        info_type = key_dict[line_to_check]
                        if info_type == 'Unmount' or info_type == 'Load':
                            df_list_dict[info_type].append(extracted_info)
                        elif info_type == 'Sensors' and 'Proximity Sensor 1' in log_lines[log_index].split(';')[-2].split(': '):
                            df_list_dict[info_type]['Proximity Sensor 1'].append(extracted_info)
                            log_index += 1
                            sensor_2_extracted_info = extract_info(log_lines[log_index])
                            if sensor_2_extracted_info:
                                df_list_dict[info_type]['Proximity Sensor 2'].append(sensor_2_extracted_info)
                            log_index += 1
                            sensor_3_extracted_info = extract_info(log_lines[log_index])
                            if sensor_3_extracted_info:
                                df_list_dict[info_type]['Proximity Sensor 3'].append(sensor_3_extracted_info)
                            log_index += 1
                elif line_to_check in key_dict and line_to_check == 'executing:Mount':
                    df_list_dict['Mount'] += 1
                    log_index += 1
                else:
                    log_index += 1
            else:
                if line_to_check in key_dict and line_to_check != 'executing:Mount':
                    log_index += 1
                elif line_to_check in key_dict and line_to_check == 'executing:Mount':
                    format_time = datetime.time.fromisoformat(my_time)
                    if format_time < end_time:
                        end_time = (datetime.datetime.combine(datetime.date.today(), format_time) + datetime.timedelta(minutes=5)).time()
                        log_index += 1
                    elif format_time > end_time:
                        df_list_dict['Mount'] += 1
                        end_time = (datetime.datetime.combine(datetime.date.today(), format_time) + datetime.timedelta(minutes=5)).time()
                        log_index += 1
                else:
                    log_index += 1
        
        runtime_log_output.write('\nFor log file: {}\n'.format(log))
        for key, value in df_list_dict.items():
            if not value:
                runtime_log_output.write('\tNo Data for: {}\n'.format(key))
                continue
            elif key != 'Mount' and key != 'Sensors':
                try:
                    make_csv_and_graph(value, key, m_vision_dir_in, log_title + '_' + key)
                    runtime_log_output.write('\tData successfully parsed for: {}\n'.format(key))
                except Exception:
                    runtime_log_output.write(value)
                    runtime_log_output.write('\tFailure in: {}\n'.format(key))
                    continue
            elif key == 'Sensors':
                sensor_count = 1
                any_sensor_data = False
                for sensor_key, sensor_value in value.items():
                    if not sensor_value:
                        continue
                    else:
                        try:
                            make_csv_and_graph(sensor_value, sensor_key, sensor_dir_in, log_title + '_' + str(sensor_count))
                            runtime_log_output.write('\tData successfully parsed for: {}\n'.format(sensor_key))
                            any_sensor_data = True
                        except Exception:
                            runtime_log_output.write(sensor_value)
                            runtime_log_output.write('\tFailure in: {}\n'.format(key))
                    sensor_count += 1
                if not any_sensor_data:
                    runtime_log_output.write('\tNo Data for: {}\n'.format(key))
            elif key == 'Mount' and int(value) > 1:
                try:
                    master_count_df = pd.read_csv(master_count_csv_in)
                    warnings.simplefilter(
                        action='ignore', category=FutureWarning)
                    if log_title not in master_count_df['Date'].values:
                        new_row = {'Date': log_title, 'Mounts': value}
                        master_count_df = master_count_df.append(
                            new_row, ignore_index=True)
                        master_count_df[['Date', 'Mounts']].to_csv(
                            master_count_csv_in)
                    runtime_log_output.write('\tData successfully parsed for: {}\n'.format(key))
                except Exception as e:
                    runtime_log_output.write(str(value) + '\n')
                    runtime_log_output.write('\tFailure in: {}\n'.format(key))
            else:
                runtime_log_output.write('\n######### Failure: Key Error ############\n')

def smooth_graphs(sensor_dir):
    sensor_csv_files = [os.path.join(sensor_dir, f) for f in os.listdir(sensor_dir) if f.split('.')[-1] == 'csv']
    count = 1
    for csv_file in sensor_csv_files:
        print('Smoothing Graph for Log: ', csv_file)
        try:
            df = pd.read_csv(csv_file)
        except Exception:
            print('\tFile {} failed\n'.format(csv_file))
            continue
        if int(len(df.index)) <= 50:
            print('\tNot enough rows # of rows: ', str(len(df.index)), '\n')
            continue
        print('\t# of rows: ', str(len(df.index)), '\n')
        x = df['Time'].values
        y = df[df.columns[3]].values
        if count % 4 == 0:
            count = 1
        else:
            count += 1
        window_size = int(len(df.index) * .33)
        y_rolling = df[df.columns[3]].rolling(window_size).mean()[window_size:]
        x_rolling = df['Time'].values[window_size:]
        
        n_rolling_ticks = len(x_rolling) // 10
        n_ticks = len(df.index) // 10

        smooth_fig, smooth_axs = plt.subplots(1)
        smooth_axs.plot(x_rolling, y_rolling)
        # smooth_axs.title('Window Size: ' + str(window_size))
        smooth_axs.xticks(plt.get_xticks()[::n_rolling_ticks])
        smooth_axs.tick_params(axis='x', rotation=-45)
        smooth_fig.savefig(os.path.join(sensor_dir, csv_file.split('.')[0] + '_smooth.png'))

        dy = max(y_rolling) - min(y_rolling)
        x_der = y_rolling**2 + 1
        dxdy = np.gradient(x_der, dy)
        
        der_fig, der_axs = plt.subplots(1)
        der_axs.plot(x_rolling, dxdy)
        der_axs.title('Derivative')
        der_axs.xticks(der_axs.get_xticks()[::n_rolling_ticks])
        der_axs.tick_params(axis='x', rotation=-45)
        der_fig.savefig(os.path.join(sensor_dir, csv_file.split('.')[0] + '_derivative.png'))
        plt.close('all')
############################################################ Run Code ############################################################

if __name__ == '__main__':
    ############################################# INITIALIZATION #############################################
    root_dir = os.getcwd()

    ############################################# Runtime Log #############################################
    runtime_root = os.path.join(os.path.join(root_dir, 'Backup_Files'), 'parse_runtime_logs')
    dir_check(runtime_root)
    runtime_log_name = 'log_' + time.strftime('%Y%m%d_%H_%M_%S', time.localtime()) + '.log'
    runtime_log_dir = os.path.join(runtime_root, runtime_log_name)
    runtime_log_output = open(runtime_log_dir, 'w')
    runtime_log_output.write('Log for run on: {}'.format(time.strftime('%m-%d-%Y %H:%M:%S', time.localtime())))
    ############################################# Log File Dir #############################################
    log_files_root = os.path.join(root_dir, 'Log_Files')
    dir_check(log_files_root)
    log_files_dir_amx = os.path.join(log_files_root, 'log_files_amx')
    dir_check(log_files_dir_amx)
    log_files_dir_fmx = os.path.join(log_files_root, 'log_files_fmx')
    dir_check(log_files_dir_fmx)
    log_files_dir_lix = os.path.join(log_files_root, 'log_files_lix')
    dir_check(log_files_dir_lix)
    ############################################# PR Log Dir #############################################
    log_pr_dir_amx = os.path.join(log_files_dir_amx, 'log_files_pr')
    dir_check(log_pr_dir_amx)
    log_pr_dir_fmx = os.path.join(log_files_dir_fmx, 'log_files_pr')
    dir_check(log_pr_dir_fmx)
    log_pr_dir_lix = os.path.join(log_files_dir_lix, 'log_files_pr')
    dir_check(log_pr_dir_lix)
    ############################################# Outter Output Dir ############################################# 
    output_dir_root = os.path.join(root_dir, 'Output_Files')
    dir_check(output_dir_root)
    output_dir_amx = os.path.join(output_dir_root, 'output_amx')
    dir_check(output_dir_amx)
    output_dir_fmx = os.path.join(output_dir_root, 'output_fmx')
    dir_check(output_dir_fmx)
    output_dir_lix = os.path.join(output_dir_root, 'output_lix')
    dir_check(output_dir_lix)
    ############################################# Inner Out Dir #############################################
    m_vision_amx, sensors_amx, sample_counter_amx = create_output_dirs(
        output_dir_amx)
    m_vision_fmx, sensors_fmx, sample_counter_fmx = create_output_dirs(
        output_dir_fmx)
    m_vision_lix, sensors_lix, sample_counter_lix = create_output_dirs(
        output_dir_lix)
    ############################################# Master CSV Files #############################################
    master_count_csv_amx = os.path.join(output_dir_amx, 'master_count_amx.csv')
    csv_check(master_count_csv_amx)
    master_count_csv_fmx = os.path.join(output_dir_fmx, 'master_count_fmx.csv')
    csv_check(master_count_csv_fmx)
    master_count_csv_lix = os.path.join(output_dir_lix, 'master_count_lix.csv')
    csv_check(master_count_csv_lix)
    ############################################# Log Files #############################################
    log_files_amx = [os.path.join(log_files_dir_amx, log) for log in os.listdir(
                     log_files_dir_amx) if log.split('.')[-1] == 'log']
    log_files_fmx = [os.path.join(log_files_dir_fmx, log) for log in os.listdir(
                     log_files_dir_fmx) if log.split('.')[-1] == 'log']
    log_files_lix = [os.path.join(log_files_dir_lix, log) for log in os.listdir(
                     log_files_dir_lix) if log.split('.')[-1] == 'log']

    total_log_files = len(log_files_amx) + len(log_files_fmx) + len(log_files_lix)

    log_files_pr_amx = [os.path.join(log_pr_dir_amx, log) for log in os.listdir(
        log_pr_dir_amx) if log.split('.')[-1] == 'log']
    log_files_pr_fmx = [os.path.join(log_pr_dir_fmx, log) for log in os.listdir(
        log_pr_dir_fmx) if log.split('.')[-1] == 'log']
    log_files_pr_lix = [os.path.join(log_pr_dir_lix, log) for log in os.listdir(
        log_pr_dir_lix) if log.split('.')[-1] == 'log']

    total_pr_log_files = len(log_files_pr_amx) + \
        len(log_files_pr_fmx) + len(log_files_pr_lix)

    if total_log_files + total_pr_log_files == 0:
        sys.exit('################# Please Add Log Files #################')
    
    ############################################# INFORMATION EXTRACTION #############################################
    print('############ Parsing FMX Log Files ############')
    parse_logs(log_files_fmx, log_pr_dir_fmx, m_vision_fmx, sensors_fmx, master_count_csv_fmx, sample_counter_fmx)
    print('############ Parsing AMX Log Files ############')
    parse_logs(log_files_amx, log_pr_dir_amx, m_vision_amx, sensors_amx, master_count_csv_amx, sample_counter_amx)
    print('############ Parsing LIX Log Files ############')
    parse_logs(log_files_lix, log_pr_dir_lix, m_vision_lix, sensors_lix, master_count_csv_lix, sample_counter_lix, lix_run=True)
    # print('############ Smoothing FMX Sensor Files ############')
    # smooth_graphs(sensors_fmx)
    # print('############ Smoothing AMX Sensor Files ############')
    # smooth_graphs(sensors_amx)
    # print('############ Smoothing LIX Sensor Files ############')
    # smooth_graphs(sensors_lix)
    runtime_log_output.close()

############################################################ EOF ############################################################