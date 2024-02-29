# This code is to search through the SAS log files to find useful information 
# Author Jianjun Dai
# Date 07-Feb-2024 
# 
# Notes:
# currently, excluding 'DW_MPD_Viib_Outer' files, as they are too big 


#!/bin/bash

# Define the folder you want to search in
folder_path_1="/saslogs/lev1/"
folder_path_2="/saslogs/lev3/"

# Define the output file to store results
output_file="output_analysis_saslogs_lev1_timing.txt"

# Define the string you want to grep for
search_string_1="real time"
search_string_2="cpu time"
# search_string_3="NOTE"

# Define the maximum age of files in days 
max_age_days=35 

# Initialize or create the output file (overwrite if it exists)
> "$output_file"

# Loop through each log file folder
for folder_path in "$folder_path_1" "$folder_path_2"; do 

  # Loop through each file in the folder
  for file in $(find "$folder_path" -type f -ctime -$max_age_days|grep -v 'DW_MPD_Viib_Outer' |sort ); do
      # Check if it's a regular file
      if [ -f "$file" ]; then
          # Get the file name
          file_name=$(basename "$file")

          # Get the file timestamp (last modification time)
          file_timestamp=$(stat -c "%y" "$file")

          # Write the file name and timestamp to the output file
          # echo "File: $folder_path$file_name, Timestamp: $file_timestamp" >> "$output_file"

          # Loop through each search string
#       for search_string in "$search_string_1" "$search_string_2" "$search_string_3" ; do
          for search_string in "$search_string_1" "$search_string_2" ; do
              # Use grep to search for the current string in the file
              full_file_name="$folder_path""$file_name"
              grep -H "$search_string" "$full_file_name"|tail -n 1 >> "$output_file" 2>/dev/null
          done
      fi
  done
done 

echo "Script completed. Results are stored in '$output_file'."
