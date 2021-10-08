import pandas as pd
import numpy as np
import os
import datetime
from calendar import monthrange
import shutil

def clean_cycle_csv(root, cycle_month_out, target_columns):
    # Empty list for the Data frames
    df_list = []
    # Using the datetime packge to create a list of all the months
    month_list = [datetime.datetime.strptime(str(i), "%m").strftime("%B") for i in range(1,13)]
    # Getting the list of CSV files located in the root directory
    csv_files = [os.path.join(root, f) for f in os.listdir(root) if f.split('.')[-1] == 'csv']
    # Columns are intentionally labeled as  None for formatting
    column_names = [
        "None", " None", "Day", "Operation", "WeekDay", " None", "AMX morning", "Local",
        "AMX afternoon", "Local", "AMX overnight", "Local", " None", "FMX morning", "Local", 
        "FMX afternoon", "Local", "FMX overnight", "Local", " None", "NYX morning", "Local",
        "NYX afternoon", "Local", "NYX overnight", "Local", " None", "Comments", " None", " None", " None"
    ]
    for my_file in csv_files:
        # Reading the CSV file to a data frame
        tmp_df = pd.read_csv(my_file, header=None)
        # Replacing all NaN values with 0
        tmp_df = tmp_df.fillna(0)
        # Removing file extensions from the filename
        my_file = my_file.split('/')[-1]
        # Getting the cycle year
        cycle_year = int(my_file.split('_')[0])
        # Finding all indexs of rows that have a non-zero value in the 2nd column ("MONTH")
        # NOTE: The 2nd column contains lines which are not equal to zero but are also 
        #       unimportant and need to be checked for the proper value below. 
        header_lines = tmp_df.loc[(tmp_df[1] != 0)].index
        for i in range(len(header_lines)):
            # If the 2nd column ("MONTH") of the row index found at the i position of the list above (header_lines) is a 
            # valid month,then the total data frame is split from that index to x and saved in a new data frame
            # where x = the number of days in that month. 
            if tmp_df.iloc[header_lines[i]][1].capitalize() in month_list:
                #################### Step 1 ####################
                # Get the month number from the temporary data frame 
                month_number = int(datetime.datetime.strptime(tmp_df.iloc[header_lines[i]][1].capitalize(), "%B").month)
                # Using month range from the calendar package to get the number of days in the month found above
                days_in_month = monthrange(cycle_year, month_number)[1]
                # Creating a new CSV filename: "2020_Cycle-1_1.csv"
                new_filename = my_file.split('.')[0] + '_' + str(month_number) + '.csv'
                # Making a new data frame from the row index above to the row index + the number of days
                # in the given month 
                month_df = tmp_df.iloc[header_lines[i]:header_lines[i] + days_in_month]
                # Setting the column names of the data frame to the list read from the 
                # file Column_mames.txt
                # NOTE: The column names must start on the 0 index and end at the maximum index
                #       of the data frame or the program will crash due to mis-matched shapes
                month_df.columns = column_names[:len(month_df.columns)]
                # Saving only the target columns
                month_df = month_df[target_columns]
                # Reset Indicies
                month_df.index = range(1, days_in_month + 1)
                # Remove all columns of only zeros
                month_df = month_df.loc[:, (month_df != 0).any(axis=0)]
                # Remove all rows of only zeros
                month_df = month_df.loc[(month_df != 0).any(axis=1)]
                for column in month_df.columns:
                    month_df[column] = month_df[column].str.split(" ", n=1, expand=True)[0]
                # Setting the data frame index to the column 'Day' and saving it 
                # in the Output directory as the new filename defined above
                month_df.to_csv(os.path.join(cycle_month_out, new_filename))
            
                #################### Step 2 ####################
                month_df = month_df.loc[:, (month_df == 'PR').any(axis=0)]
                month_df = month_df.loc[(month_df == 'PR').any(axis=1)]
                if len(month_df.index) >= 1:
                    df_list.append([my_file.split('_')[-1].split('.')[0], cycle_year, month_number, month_df])
    return df_list

def find_pr_times(df_list, fmx_pr_csv_path, amx_pr_csv_path, target_columns, pr_csv_columns):
    time_dict = {'morning':'09:00-15:00', 'afternoon':'16:00-22:00', 'overnight':'22:00-7:00'}
    fmx_new_rows = False
    amx_new_rows = False
    for inner_list in df_list:
        cycle = inner_list[0]
        year = inner_list[1]
        month = inner_list[2]
        df = inner_list[3]
        for col_name, col_data in df.iteritems():
            beam_line = col_name.split(' ')[0]
            if beam_line == 'FMX':
                master_csv = pd.read_csv(fmx_pr_csv_path)
                master_csv_path = fmx_pr_csv_path
            elif beam_line == 'AMX':
                master_csv = pd.read_csv(amx_pr_csv_path)
                master_csv_path = amx_pr_csv_path
            else:
                print('*****ERROR*****\nIncorrect beam line!')
                continue
            time_window = time_dict[col_name.split(' ')[-1]]
            for day in df.loc[col_data == 'PR'].index:
                new_row = {'Cycle': cycle, 'Year': year, 'Month': month, 'Day': day, 'Time': time_window}
                if not ((master_csv['Cycle'] == cycle) & (master_csv['Year'] == year) & 
                        (master_csv['Month'] == month) & (master_csv['Day'] == day) &
                        (master_csv['Time'] == time_window)).any():
                    master_csv = master_csv.append(new_row, ignore_index=True)
                    master_csv = master_csv.sort_values(['Year', 'Month', 'Day', 'Time'], ascending=['True', 'True', 'True', 'True'])
                    master_csv[pr_csv_columns].to_csv(master_csv_path)
                    if beam_line == 'FMX':
                        fmx_new_rows = True
                    elif beam_line == 'AMX':
                        amx_new_rows = True
    if fmx_new_rows:
        print('New rows added to FMX PR cycle tracker')
    if amx_new_rows:
        print('New rows added to AMX PR cycle tracker')
    if not amx_new_rows and not fmx_new_rows:
        print('No new rows added, all PR cycle information already collected')

def main():
    root = os.path.join(os.getcwd(), 'Cycle_Files')
    cycle_month_out = os.path.join(root, 'Output')
    fmx_pr_csv_path = os.path.join(os.path.join(os.getcwd(), 'Output_Files'), 'FMX_PR_Tracker.csv')
    amx_pr_csv_path = os.path.join(os.path.join(os.getcwd(), 'Output_Files'), 'AMX_PR_Tracker.csv')
    pr_csv_columns = ['Cycle', 'Year', 'Month', 'Day', 'Time']
    if not os.path.exists(fmx_pr_csv_path):
        pd.DataFrame(columns=pr_csv_columns).to_csv(fmx_pr_csv_path)
    if not os.path.exists(amx_pr_csv_path):
        pd.DataFrame(columns=pr_csv_columns).to_csv(amx_pr_csv_path)
    # Creating the output directory where all newly created CSV files will go
    if not os.path.exists(cycle_month_out):
        os.makedirs(cycle_month_out)
    # Target columns are the only columns being kept in the CSV file
    target_columns = ['AMX morning', 'AMX afternoon', 'AMX overnight', 'FMX morning', 'FMX afternoon', 'FMX overnight']
    df_list = clean_cycle_csv(root, cycle_month_out, target_columns)
    find_pr_times(df_list, fmx_pr_csv_path, amx_pr_csv_path, target_columns, pr_csv_columns)
    return fmx_pr_csv_path, amx_pr_csv_path

if __name__ == '__main__':
    main()