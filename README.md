# list_cloudpools_files
Query OneFS Platform API to get list of files that have been CloudPool'd.
Run from a cluster node directly (not remotely).

- Username is required.  This is used in the HTTP request.
- If password is not provided in command, you will be prompted to provide one.
- Script attempts to detect local IP address to use for query.  If you getan error message, you can enter this manually.
- You can use `--show-count` to get a total file count of all archived files without printing the file names.
- The commands/queries may take quite a bit of time for file systems with a large number of archived files.

