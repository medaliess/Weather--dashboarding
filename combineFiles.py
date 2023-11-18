import os

# Directory containing the files
directory="./downloads/SAFI_meteo/"

# List to store the file contents
file_contents = []

# Loop through the files
for filename in os.listdir(directory):
    if filename.startswith("601850-99999-") and int(filename.split("-")[-1].split(".")[0]) in range(1949, 2024):
        with open(os.path.join(directory, filename), 'r') as file:
            file_contents.append(file.read())

# Combine the contents
combined_text = "\n".join(file_contents)

# Write the combined text to a new file
with open("combined_file.txt", "w") as combined_file:
    combined_file.write(combined_text)
