
# enhance to see how to remove data _null_;

import sys
import re

# Function to extract PROC SQL and DATA steps from SAS code
def extract_sql_and_data_steps_from_file(file_path):
    output_code = []
    
    with open(file_path, 'r') as file:
        # Read SAS code line by line
        for line in file:
            words = line.split('\t');
            line_with_lnum = line
            line_content = ' '.join(words[1:]);

            # Find the start of PROC SQL or DATA step 
            if ( re.match(r'\bproc sql;', line_content.strip())     \
                 or re.match(r'\bPROC SQL;', line_content.strip())  \
                 or re.match(r'\bdata .*?;', line_content.strip())  \
                 or re.match(r'\bDATA .*?;', line_content.strip()) ) \
               and not re.match(r'\bdata _null_; .*?;', line_content):
               output_code.append(words[0]+ ' ' + line_content.strip())

               # Get the content of PROC SQL or DATA step 
               for next_line in file:
                  line_with_lnum = next_line
                  words = next_line.split('\t');
                  line_content = ' '.join(words[1:]);
  
                  output_code.append(next_line.strip())

                  # Test if it is the end of the PROC SQL or DATA step 
                  if re.match(r'\bquit;', line_content.strip())     \
                     or re.match(r'\bQUIT;', line_content.strip())  \
                     or re.match(r'\brun;', line_content.strip())   \
                     or re.match(r'\bRUN;', line_content.strip()) :
                     # print('test to find the end, line_content is ' + line_content)
                     break
                
    return output_code

# Sample file path
# file_path = 'wln_sample2.sas'
# file_path = 'withlinenum_sample_code.sas'
file_path=sys.argv[1]

# Extract SQL and DATA steps from the file
output_detail = extract_sql_and_data_steps_from_file(file_path)

# Print extracted steps with line numbers
print("Output Detail:")
for item in output_detail:
    print(item)

