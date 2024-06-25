# Alert project

Parse log from csv file. Contains LogParser class and methods get_df_with_id. alert_message.
get_df_with_id - return dataframe with IDs according to the rule, who can be set. Parameters:
* date - column from dataframe from where the date is extracted.
* group_key - the time period for which the logs will be read. Takes values: day, month, year, minute, hour, second.
* number - the number of messages through which the notification will occur.
* column - optional parameter, name of column, for tracking the actions of a specific user.
* value - one of the colum values for tracking actions.

alert_message - function thats send info about fatal logs to the console.

## Installation

### Prerequisites
You need to have docker installed on your machine.</

Build and launch containers of the docker compose:
```bash
docker compose up --build
```