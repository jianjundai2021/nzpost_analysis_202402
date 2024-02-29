# Example line

file_path = 'txtln.txt'
with open(file_path, 'r') as file:

    for line_num, line in enumerate(file, start=1):

        # Iterate over each character in the line
        for char in line:
            # Print the character along with its ASCII value
            print("Character:", char, "ASCII Value:", ord(char))

