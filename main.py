import pandas as pd
import logging
import os

from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

LOGS = (
    "error_code",
    "error_message",
    "severity",
    "log_location",
    "mode",
    "model",
    "graphics",
    "session_id",
    "sdkv",
    "test_mode",
    "flow_id",
    "flow_type",
    "sdk_date",
    "publisher_id",
    "game_id",
    "bundle_id",
    "appv",
    "language",
    "os",
    "adv_id",
    "gdpr",
    "ccpa",
    "country_code",
    "date",
)
FILEPATH = os.environ.get("FILEPATH")


class LogParser:
    """
    Get logs from csv file and parse them.
    Sand information to the console.
    """

    df: pd.DataFrame

    def __init__(self, path, logs) -> None:
        self.logs = logs
        self.path = path

        self.df = pd.read_csv(FILEPATH, names=logs, low_memory=False, skiprows=1)
        self.df["id"] = self.df.index
        self.time_transform("sdk_date")

    def time_transform(self, datefield: str) -> None:
        """
        Transform any time of date to the %d:%m:%Y:%H:%M:%S" format.

        Args:
            datefield (str): name of the date column in the csv.
        """
        self.df[datefield] = (
            self.df[datefield]
            .str.replace("[^0-9./: -]", "", regex=True)
            .str.replace("[^0-9]", ":", regex=True)
            .str.split(":")
            .str[:6]
        )

        for date in self.df[datefield]:
            for zero_time in range(len(date)):
                #fills in the empty spaces in the date
                if date[zero_time] == "":
                    date[zero_time] = "01"

            #swaps the year and day
            if len(date[0]) == 4:
                date[0], date[2] = date[2], date[0]
            #adds a century
            if len(date[2]) == 2:
                date[2] = "20".join(date[2])
            #swap the day and month
            if int(date[1]) > 12:
                date[0], date[1] = date[1], date[0]

        self.df[datefield] = pd.to_datetime(
            self.df[datefield].str.join(":"), format="%d:%m:%Y:%H:%M:%S"
        )

    def get_df_with_id(
        self, date_column: str, time_period: str, number: int, column=None, value=None
    ) -> pd.DataFrame:
        """
        Get from dataframe indices according to the rule. For a specified period of time
        and the number of messages after which you need to notify. So it can take the name of the column and
        the value from it for detailed tracking of actions.

        Args:
            date_column (str): column from dataframe from where the date is extracted
            time_period (str): the time period for which the logs will be read. Takes values: day, month, year, minute, hour, second
            number (int): the number of messages through which the notification will occur.
            column (_type_, optional): optional parameter, name of column, for tracking the actions of a specific user. Defaults to None.
            value (_type_, optional): one of the colum values for tracking actions.. Defaults to None.

        Returns:
            pd.DataFrame: dataframe wit indices.
        """
        df_indices: pd.DataFrame

        if column is None and value is None:
            df_indices = self.df
        elif column is not None and value is not None:
            if value in self.df[column].unique():
                df_indices = self.df[self.df[column] >= value]
            else:
                logging.error(
                    f"Column {column} doesn't have value {value}", exc_info=True
                )
        else:
            logging.error(
                f"Parameters column and value must be specified", exc_info=True
            )

        separated_date = [
            self.df[date_column].dt.day,
            self.df[date_column].dt.month,
            self.df[date_column].dt.year,
            self.df[date_column].dt.hour,
            self.df[date_column].dt.minute,
            self.df[date_column].dt.second,
        ]

        grouped_values = {
            "day": separated_date[0],
            "month": separated_date[:1],
            "year": separated_date[:2],
            "hour": separated_date[:3],
            "minute": separated_date[:4],
            "second": separated_date[:5],
        }

        if time_period not in grouped_values:
            logging.error(
                f"group_key can only take values: day, month, year, hour, minute or second",
                exc_info=True,
            )

        df_indices = df_indices.groupby(grouped_values[time_period])["id"].apply(list)

        df_indices = df_indices[df_indices.str.len() >= number]

        return df_indices

    def alert_message(
        self, date_column: str, time_period: str, number: int, column=None, value=None
    ) -> None:
        """
        Write log to the the console.

        Args:
            date_column (str): column from dataframe from where the date is extracted
            time_period (str): the time period for which the logs will be read. Takes values: day, month, year, minute, hour, second
            number (int): the number of messages through which the notification will occur.
            column (_type_, optional): optional parameter, name of column, for tracking the actions of a specific user. Defaults to None.
            value (_type_, optional): one of the colum values for tracking actions.. Defaults to None.

        """
        df_indices = self.get_df_with_id(
            date_column, time_period, number, column, value
        )
        logging.info(f"During {time_period} you have {len(df_indices)} alert messages!")

        for index_list in df_indices:
            logging.info(f"\tLogs number: {len(index_list)}")


if __name__ == "__main__":
    error_parser = LogParser(FILEPATH, LOGS)
    error_parser.df = error_parser.df[
        (error_parser.df["severity"] == "Error") & (error_parser.df["error_code"] > 0)
    ]

    error_parser.alert_message("sdk_date", "minute", 10)
    error_parser.alert_message(
        "sdk_date", "hour", 10, "bundle_id", "com.pregnantcatemma.virtualpet"
    )
