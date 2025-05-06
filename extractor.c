#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <zip.h>
#include <sys/stat.h>
#include <dirent.h>

#define MAX_PASSWORD_LENGTH 256

// Function to create directories recursively
int create_directory(const char *path) {
    char temp_path[256];
    char *pos = NULL;

    // Copy path to temp_path and make directories one by one
    snprintf(temp_path, sizeof(temp_path), "%s", path);
    for (pos = temp_path + 1; *pos; pos++) {
        if (*pos == '/') {
            *pos = '\0'; // Temporarily terminate the string to create the directory
            mkdir(temp_path, 0755);
            *pos = '/'; // Restore the '/' to continue
        }
    }

    // Finally, create the last directory in the path
    return mkdir(temp_path, 0755);
}

// Function to extract a ZIP file with password
int extract_zip_with_password(const char *zip_filename, const char *password) {
    int err = 0;
    zip_t *archive = NULL;
    zip_file_t *zfile = NULL;
    struct zip_stat stat;
    char out_filename[256];
    FILE *outfile = NULL;
    unsigned char buffer[1024];
    size_t bytes_read;

    // Open the outer ZIP archive
    archive = zip_open(zip_filename, 0, &err);
    if (archive == NULL) {
        fprintf(stderr, "Error opening ZIP file: %d\n", err);
        return -1;
    }

    // Loop through each file in the outer ZIP archive
    for (int i = 0; i < zip_get_num_entries(archive, 0); i++) {
        if (zip_stat_index(archive, i, 0, &stat) == 0) {
            // Try to open the file within the archive
            zfile = zip_fopen_index_encrypted(archive, i, 0, password);
            if (zfile == NULL) {
                fprintf(stderr, "Error opening file inside ZIP: %d\n", err);
                zip_close(archive);
                return -1;
            }

            // Check if it's a directory (directories in ZIP archives end with '/')
            if (stat.name[strlen(stat.name) - 1] == '/') {
                // It's a directory, create it
                printf("Creating directory: %s\n", stat.name);
                create_directory(stat.name);
                zip_fclose(zfile);
                continue;
            }

            // Prepare output filename (same name as inside the ZIP)
            snprintf(out_filename, sizeof(out_filename), "%s", stat.name);

            // Open output file to write extracted content
            outfile = fopen(out_filename, "wb");
            if (outfile == NULL) {
                fprintf(stderr, "Error opening output file for %s\n", stat.name);
                zip_fclose(zfile);
                zip_close(archive);
                return -1;
            }

            // Read the file from the ZIP archive and write it to disk
            while ((bytes_read = zip_fread(zfile, buffer, sizeof(buffer))) > 0) {
                fwrite(buffer, 1, bytes_read, outfile);
            }

            // Clean up
            fclose(outfile);
            zip_fclose(zfile);

            printf("Extracted: %s\n", out_filename);

            // If the extracted file is a ZIP file, extract it as well
            if (strstr(out_filename, ".zip") != NULL) {
                printf("Found another ZIP file: %s\n", out_filename);
                extract_zip_with_password(out_filename, password); // Recursively extract inner ZIP file
            }
        }
    }

    zip_close(archive);
    return 0;
}

// Function to search for any file that starts with "full_log" in the current directory
int find_full_log_zip_file(const char *password) {
    DIR *dir;
    struct dirent *entry;

    dir = opendir(".");
    if (dir == NULL) {
        perror("Failed to open directory");
        return -1;
    }

    // Look for files that start with "full_log" and end with ".zip"
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, "full_log", 8) == 0 && strstr(entry->d_name, ".zip") != NULL) {
            printf("Found ZIP file: %s\n", entry->d_name);
            // Extract the found ZIP file
            int result = extract_zip_with_password(entry->d_name, password);
            if (result != 0) {
                fprintf(stderr, "Failed to extract ZIP file: %s\n", entry->d_name);
            }
        }
    }

    closedir(dir);
    return 0;
}

int main() {
    char password[MAX_PASSWORD_LENGTH];

    // Ask user for password (no need for filename input anymore)
    printf("Enter password for the ZIP file: ");
    scanf("%s", password);

    // Find the ZIP file that starts with "full_log" and extract it
    int result = find_full_log_zip_file(password);
    if (result == 0) {
        printf("Extraction completed successfully.\n");
    } else {
        printf("Extraction failed.\n");
    }

    return 0;
}

