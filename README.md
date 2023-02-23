A simple ETL framework for Python, SQL and BAT files which uses a Postgres database for activity logging.
---

## Local windows pc install

### first install Postgres

Download and install postgres (https://www.postgresql.org/download/) to your local computer.  Remember the password.  After installing Postgres, your connection string will be:

> - host: localhost
> - port: 1532
> - name: postgres
> - schema: public
> - Username: postgres  
> - Password: <whatever_you_supplied>

### download zetl and unzip to a local folder on your computer, such as c:\zetl
  
Folder will look like this:
  
> - zetl
> - zetl\examples
> - zetl\install_ddl
> - zetl\zetl_scripts

- run zetl\setup.bat
  
This will install the neccessary python packages and prompt for connection details to the Postgres database you just istalled. Hit enter to accept the defaults and enter the password you entered during the database setup.
  
setup will then connect to postgres and create the 3 tables required by zetl.
  
### check your setup
  
- Run zetl.py
  
If your setup is successful, zetl will connect and list etl's available to run such as:
  
> demo1
> demo2
> demo3
> empty_log

You can try running an ETL by passing it as an argument to zetl as follows:
  
- zetl demo1

## Usage

### What is an ETL to zetl ?

- An ETL exists in the form of a directory, under zetl_scripts, with files of a specific naming convention which are either python, windows bat, or sql.
- The file naming convention is as follows: step_number.activity.extension
  
> - step_number is any integer unique in the immediate folder
> - activity is any alphanumeric name for the activity of the file
> - extension must be either py, bat or sql

- For example:
  
> - zetl\zetl_scripts\demo1\1.hello.py
> - zetl\zetl_scripts\demo1\2.something.sql
> - zetl\zetl_scripts\demo1\3.hello.bat

