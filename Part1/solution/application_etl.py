import pandas as pd


class ApplicationLifecycle:
    """
    This class transforms periodic data from Application Lifecycles

    Parameters
    -----------
    app_filepath: str
        Input file with raw data for application lifecycles

    process_date: datetime
        Current date when etl was executed. Used as string in the output filename.

    Returns
    -------
    final_df: pd.DataFrame
        Dataframe with final transformations. This is saved to local disk.
    """

    def __init__(self, app_filepath=None, process_date=None):

        self.app_filepath = app_filepath

        if process_date is None:
            self.process_date = pd.to_datetime('today').date()
        else:
            self.process_date = process_date

    def read_csv_files(self, csv_filepath):
        """
        Function to read .csv input files.

        Parameter
        ----------
        csv_filepath: str
            Input .csv file for application lifecycle data

        Return
        -------
        pd.DataFrame
            Dataframe of the input file.
        """
        return pd.read_csv(csv_filepath, header=0)

    def transform_app_data(self):
        """
        Transforms application lifecycle dataframe
        """
        app_df = self.read_csv_files(self.app_filepath)

        # Split data on '|' char to separate column, stack events as separate rows,
        # replace characters in events to ease transformations.
        stacked_df = pd.DataFrame(app_df.string_agg.str.split('|', expand=True, ).stack())[0].\
            apply(lambda x: str(x).replace(']', '=').replace('[', ''))
        stacked_df = pd.DataFrame(stacked_df)
        stacked_df.reset_index(inplace=True)
        stacked_df.set_index('level_0', inplace=True)
        stacked_df = stacked_df.rename(columns={0: 'data'})

        # Concatenate untransformed dataframe to stacked dataframe
        df_concat_stack = pd.concat([app_df, stacked_df], axis=1)

        # Separate events from timestamps as columns
        df_concat_stack['event'] = df_concat_stack['data'].apply(lambda x: str(x).split('=')[0])
        df_concat_stack['event_timestamps'] = df_concat_stack['data'].apply(lambda x: str(x).split('=')[1])

        # Select columns of interest and convert timestamp to desired format
        df_concat_stack = df_concat_stack[['UniqueID', 'account_type', 'event', 'event_timestamps']]
        df_concat_stack.reset_index(inplace=True)
        df_concat_stack = df_concat_stack[['UniqueID', 'event', 'event_timestamps']]
        df_concat_stack['event_timestamps'] = df_concat_stack['event_timestamps'].\
            apply(lambda x: pd.to_datetime(str(x), format='%Y-%m-%d %H:%M').strftime("%b %d %Y, %H:%M:%S"))

        """Unpack contents of dataframe into dictionary."""
        data_dict = {}

        # Iterate over rows in the dataframe
        for i, row in df_concat_stack.iterrows():
            uniqueID = row[df_concat_stack.columns.get_loc('UniqueID')]
            event = str(row[df_concat_stack.columns.get_loc('event')])
            event_ts = str(row[df_concat_stack.columns.get_loc('event_timestamps')])
            counter = 0

            # if uniqueID does not exist create new key as uniqueID and assign value(dictionary of event: timestamp)
            if uniqueID not in data_dict.keys():
                data_dict[uniqueID] = {event + '_' + str(counter): event_ts}

            # if uniqueID exists but event does not exist,
            # create new key as event and assign timestamp value
            elif uniqueID in data_dict.keys() and event + '_' + str(counter) not in data_dict[uniqueID].keys():
                data_dict[uniqueID][event + '_' + str(counter)] = event_ts

            # if uniqueID and event exists, compare event to event keys,
            # append counter to event, create new key as event_counter and assign timestamp value
            elif uniqueID in data_dict.keys() and event + '_' + str(counter) in data_dict[uniqueID].keys():
                key_list = []
                for k in data_dict[uniqueID].keys():
                    if event in str(k):
                        val = k.split('_')[-1]
                        key_list.append(val)

                counter = int(max(key_list)) + 1
                data_dict[uniqueID][event + '_' + str(counter)] = event_ts

        # Create DataFrame from dictionary and transpose final dataframe
        final_df = pd.DataFrame(data_dict).transpose()
        final_df.reset_index(inplace=True)
        final_df = final_df.rename(columns={'index': 'UniqueID'})

        # Select columns of interest
        final_df = final_df[['UniqueID', 'REGISTERED_0', 'ACKNOWLEDGED_0', 'APPROVED_0', 'REACKNOWLEDGED_0',
                             'CLOSED_0', 'APPOINTMENT_SCHEDULED_0', 'REJECTED_0', 'ON_HOLD_0', 'BLOCKED_0',
                             'TERMINATE_0', 'INITIATED_0', 'APPROVED_1', 'ON_HOLD_1', 'INITIATED_1', 'REGISTERED_1',
                             'BLOCKED_1', 'CLOSED_1', 'APPROVED_2']]

        # Save output to storage
        final_df.to_csv('OutputApplicationLifecycle' + '_' + str(self.process_date) + '.csv')

        return final_df
