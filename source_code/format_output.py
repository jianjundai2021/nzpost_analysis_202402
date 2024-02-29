import csv
import os

# Replace 'yourfile.csv' with the path to your CSV file
filename = 'sas_job_detail.csv'
output_folder = '.'
# output_file = 'sas_job_detail_formatted.txt'

last_process_flow_name = None
last_sub_process_flow_name = None
outfile = None

with open(filename, 'r', newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        process_flow_name = row['LSF_PROCESS_FLOW_NAME']
        sub_process_flow_name = row['LSF_SUBPROCESS_NAME']
        sasjob = row['LSF_SAS_CODE_NAME']
        tbls = row['TABLE_OPERATION_LIST']
		
        # Check if the nation has changed since the last row
        if process_flow_name != last_process_flow_name:
            if outfile is not None:
                outfile.close()
				 
            output_filename = os.path.join(output_folder, process_flow_name+'.txt')
            outfile = open(output_filename, 'w', encoding='utf-8')
            outfile.write(process_flow_name+'\n')
            last_process_flow_name = process_flow_name
            last_sub_process_flow_name = None  # Reset the last region for the new nation

        # Check if the region has changed since the last row
        if sub_process_flow_name != last_sub_process_flow_name:
            outfile.write("  " + sub_process_flow_name+'\n')
            last_sub_process_flow_name = sub_process_flow_name

        # Print the city, further indented
        outfile.write("    " + sasjob+'\n')
        outfile.write("    " + tbls+'\n')
