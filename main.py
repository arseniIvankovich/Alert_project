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
    df: pd.DataFrame

    def __init__(self, path, logs) -> None:
        self.logs = logs
        self.path = path

        self.df = pd.read_csv(
            FILEPATH, names=logs, low_memory=False, skiprows=1
        )
        self.df["id"] = self.df.index
        self.time_transform("sdk_date")

    def time_transform(self, field: str) -> None:
        self.df[field] = (
            self.df[field]
            .str.replace("[^0-9./: -]", "", regex=True)
            .str.replace("[^0-9]", ":", regex=True)
            .str.split(":")
            .str[:6]
        )

        for date in self.df[field]:
            for zero_time in range(len(date)):
                if date[zero_time] == "":
                    date[zero_time] = "01"

            if len(date[0]) == 4:
                date[0], date[2] = date[2], date[0]
            if len(date[2]) == 2:
                date[2] = "20".join(date[2])
            if int(date[1]) > 12:
                date[0], date[1] = date[1], date[0]

        self.df[field] = pd.to_datetime(
            self.df[field].str.join(":"), format="%d:%m:%Y:%H:%M:%S"
        )

    def get_id_df(
        self, date: str, group_key: str, number: int, column=None, value=None
    ) -> pd.DataFrame:

        df_indices: pd.DataFrame

        if column is None and value is None:
            df_indices = self.df
        elif column is not None and value is not None:
            if value in self.df[column].unique():
                df_indices = self.df[self.df[column] == value]
            else:
                logging.error(
                    f"Column {column} doesn't have value {value}", exc_info=True
                )
        else:
            logging.error(
                f"Parameters column and value must be specified", exc_info=True
            )

        separated_date = [
            self.df[date].dt.day,
            self.df[date].dt.month,
            self.df[date].dt.year,
            self.df[date].dt.hour,
            self.df[date].dt.minute,
            self.df[date].dt.second,
        ]

        grouped_values = {
            "day": separated_date[0],
            "month": separated_date[:1],
            "year": separated_date[:2],
            "hour": separated_date[:3],
            "minute": separated_date[:4],
            "second": separated_date[:5],
        }

        if group_key not in grouped_values:
            logging.error(
                f"group_key can only take values: day, month, year, hour, minute or second",
                exc_info=True,
            )

        df_indices = df_indices.groupby(grouped_values[group_key])[
            "id"].apply(list)

        df_indices = df_indices[df_indices.str.len() >= number]

        return df_indices

    def alert_message(
        self, date_column: str, date: str, number: int, column=None, value=None
    ) -> None:

        df_indices = self.get_id_df(
            date_column, date, number, column, value)
        logging.info(f"During {date} you have {len(df_indices)} alert messages!")

        for index_list in df_indices:
            logging.info(f"\tAlert number: {len(index_list)}")


if __name__ == "__main__":
    error_parser = LogParser(FILEPATH, LOGS)
    error_parser.df = error_parser.df[error_parser.df["severity"] == "Error"]

    error_parser.alert_message("sdk_date", "minute", 10)
    error_parser.alert_message(
        "sdk_date", "hour", 10, "bundle_id", "com.pregnantcatemma.virtualpet"
    )
