A simple ETL framework for Python, SQL and BAT files which uses a Postgres database for activity logging.
the zetl framework requires python and Postgres to run.

---

### 1. Install Python

Download and install python (https://www.python.org/downloads/) to your local computer.

### 2. Install Postgres

Download and install postgres (https://www.postgresql.org/download/) to your local computer.  Remember the password.  
When you run zetl it will prompt you for database connection details.  At the end of prompting, it will ask if you want
to save the connection details (y/n).  If you select y, the details are saved in that folder and you aren't prompted again
unless the details fail on connect. 

Here are the defaults for postgtres:

> - host: localhost
> - port: 1532
> - name: postgres
> - schema: public
> - Username: postgres  
> - Password: <whatever_you_supplied>

### 3.Install zetl 

Just install with pip

```
pip install zetl
```
  
Wherever you run zetl, it will look for a folder called zetl_scripts, where all your etl folders are stored.
  
> zetl_scripts

In the tests folder on git hub you can see examples of etl folders, and etl scripts under the zetl_scripts folder.

>
> zetl_scripts\demo1
> zetl_scripts\demo2
> zetl_scripts\demo3
> zetl_scripts\empty_log
> zetl_scripts\view_log
>


### 3. Run zetl

```
py -m zetl.run
```
  
This prompt for connection details to the Postgres database you just istalled. 
Hit enter to accept the defaults and enter the password you entered during the database setup.
  
 
### 4. Run zetl commands

To run any zetl commands, go to the command line and change to the zetl directory.  eg. CD \zetl

If your setup is successful, when you run zetl.py with no parameters, it will connect and list ETL's available to run such as:
  
> - demo1
> - demo2
> - demo3
> - view_log
> - empty_log

--- 

### Usage

--- 

### What is an ETL in the zetl framework ?

An ETL exists in the form of a directory, under zetl_scripts, with files of a specific naming convention which are either python, windows bat, or sql.  The file naming convention is as follows: step_number.activity.extension
  
> - **step_number** is any integer unique in the immediate folder
> - **activity** is any alphanumeric name for the activity of the file
> - **extension** must be either py, bat or sql

####  For example:
  
> - zetl\zetl_scripts\demo1\1.hello.py
> - zetl\zetl_scripts\demo1\2.something.sql
> - zetl\zetl_scripts\demo1\3.hello.bat

### create an ETL

create a folder under zetl_scripts and add a file which follows the naming convention step_number.activity.extension
For example:

- 1.anything.sql
- 2.anythingelses.bat
- 3.something.py

### run an ETL

Go to the command line and change to the zetl directory.  eg. CD \zetl
pass the ETL as a parameter to zetl

for example:

> zetl demo1

### View the ETL Log

Everytime an ETL runs, the z_log table is updated with the activity.  To see view the log, query the z_log table or run the ETL view_log as follows:

> zetl view_log

