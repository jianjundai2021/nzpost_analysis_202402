#   get the recent run job sas code name list from SQL 
#   done manually via SQL Developer
#
#   put it to a file and save it to linux 
#   done manually 
#   use shell script to loop through this list 
#   each time, copy the file from /sas/lev1 or lev3 to working folder
#   output to file with line number
#   done manually 


#   call python job to identify proc sql and data step 

HOME_DIR=/home/jxd01/qrious_study/jianjundai/sasdicode_stripe
SOURCE_DIR=$HOME_DIR/sas_code_folder_simple/Lev1
OUTPUT_DIR=$HOME_DIR/sas_code_folder_simple_part3/

grep -i "execute" $SOURCE_DIR/*.sas > $OUTPUT_DIR/grep_execute_lev1.txt


SOURCE_DIR=$HOME_DIR/sas_code_folder_simple/Lev3
OUTPUT_DIR=$HOME_DIR/sas_code_folder_simple_part3/

grep -i "execute" $SOURCE_DIR/*.sas > $OUTPUT_DIR/grep_execute_lev3.txt



