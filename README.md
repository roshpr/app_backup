Backup infrastructure applications
* The code was written as a sample project for backup and restore my me and Prakash.
* The application has 2 components, one backup agent and other backup manager (Prakash wrote this)
* Backup Agent is plugin based. You will just need to drop in your application plugin to support its backup and restore.

Setup
* The application requires you to run the ssh_copy_id.sh with the node ips where you want to install the agents.
* Run the sync_nodes.sh script to copy all required files to the application on nodes to be backed up.
* copy the sample backup.yaml file to set the required settings for backup
* Once the settings yaml is created you will need to use it for backup and restore
* Currently we have not exposed a command line options for running the script instead you can change the options by editing the file manager/backupmanager.py
* Change the last line in the file to mention which settings file to use
* You can change the backup directory name and backup name
* -n: you backup name
* -r: if you speciffy this flag the restore of application is triggered else backup
* -S: While backup we autocreate backup snapshot name for a given backup name. This needs to be provided during restore
* -t: backup directory name
* -c: settings file path

* Sample restore: args.extend(['-n', 'dailyjuly19', '-c', '../examples/example.yaml', '-t', '/backups', '-r', '-S', '2017-07-12T17:42:34'])
