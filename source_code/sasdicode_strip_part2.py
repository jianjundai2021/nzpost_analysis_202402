
# enhance to see how to remove data _null_;

import sys
import re

# Function to extract PROC SQL and DATA steps from SAS code
def extract_sql_and_data_steps_from_file(file_path):
    output_code = []
    
    with open(file_path, 'r') as file:
        # Read SAS code line by line
      for line in file:
        # print "the line content is: ", line 
        words = line.split(' ');
        line_with_lnum = line
        line_content = ' '.join(words[1:]);
        # Find the start of PROC SQL or DATA step 
        if ( re.search(r'\bmerge into', line_content.strip())     \
             or re.search(r'\bMERGE INTO', line_content.strip())) :
             # print "here 1", line_content 
             output_code.append(words[0]+ ' ' + line_content.strip())
             # print "the output_code 1 is: ", output_code

             # Get the next line of code
             loop_cnt =1 
             for next_line in file:
               line_with_lnum = next_line
               words = next_line.split('\t');
               line_content = ' '.join(words[1:]);
               output_code.append(next_line.strip())
               loop_cnt = loop_cnt + 1 
               if loop_cnt > 1: 
                  break

    output_code_2 =file_path + ':'
    for item in output_code:
        words = item.split(' ')
        output = ' '.join(words[1:]);
        output_code_2 = output_code_2 + output
        if output != "MERGE INTO":
           output_code_2 = output_code_2 + '\n' + file_path + ':'

      
                
    return output_code_2

# Sample file path
# file_path = 'wln_sample2.sas'
# file_path = 'withlinenum_sample_code.sas'
file_path=sys.argv[1]

# Extract SQL and DATA steps from the file
output_detail = extract_sql_and_data_steps_from_file(file_path)

# Print extracted steps with line numbers
# print("Output Detail:")
print(output_detail)
# for item in output_detail:
#     print(item)

