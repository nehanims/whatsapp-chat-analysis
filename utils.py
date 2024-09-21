'''
## Cleanup
- clean up emojis - use the emoji library
- handle nbsp, lrm and other special characters
- clean up hindi charaters and other languages
- merge multiple lines into one if they dont start with a [timestamp] pattern
'''

import emoji
import pandas as pd
from dateutil import parser


def clean_file(file_path):
    cleaned = None
    with open(file_path, 'r', encoding='utf-8') as file:
        lines = pd.Series(file.readlines())
        cleaned = (lines
                   .pipe(clean_special_chars)
                   .pipe(clean_emojis)
                   .pipe(convert_didi_to_shalini)
                   .pipe(remove_newlines)
                   .pipe(merge_multiline_messages)
                   .pipe(split_messages)
                   )

    return cleaned


def clean_emojis(series_lines):
    return series_lines.apply(emoji.demojize)


def clean_special_chars(series_lines):
    # clean up nbsp, lrm and other special characters
    # TODO this seems to be working fine, but need to test it more to be sure that it is not removing any characters that are actually needed
    special_chars_pattern = r'[\u00A0\u200E\u200F\u202A-\u202F]'
    return series_lines.str.replace(special_chars_pattern, '', regex=True)


def clean_hindi_chars(series_lines):
    pass


def remove_newlines(series_lines):
    return series_lines.str.replace('\n', ' ', regex=True)


def merge_multiline_messages(series_lines):
    timestamp_pattern = r'^\[\d{1,2}\/\d{1,2}\/\d{1,4}, \d{1,2}:\d{1,2}:\d{1,2}((PM|AM))?\]'
    has_timestamp = series_lines.str.match(timestamp_pattern)
    groups = has_timestamp.cumsum()
    return series_lines.groupby(groups).agg(' '.join)


'''
## Standardize
- convert to lowercase
- make sure the date format matches in all files
- convert didi to shalini in my logs
'''


def convert_to_lowercase(series_lines):
    return series_lines.str.lower()


def match_date_format(series_lines):
    pass


def convert_didi_to_shalini(series_lines):
    return series_lines.str.replace('Didi', 'Shalini', regex=True)


'''
## Split the messages into dataframe columns
- datetime
- sender
- message
'''


def parse_date(date_string):
    try:
        return parser.parse(date_string, dayfirst=False)
    except:
        try:
            return parser.parse(date_string, dayfirst=True)
        except:
            return pd.NaT


def split_messages(cleaned_series):
    # Convert the series to a dataframe
    message_pattern = r'^\[\d{1,2}\/\d{1,2}\/\d{1,4}, \d{1,2}:\d{1,2}:\d{1,2}\w*\][\s\w]*:(.+)$'
    df = pd.DataFrame({'full_message': cleaned_series})
    df['date_time'] = df['full_message'].str.extract(r'\[(.*?)\]', expand=False)
    df['sender'] = df['full_message'].str.extract(r'\] ([^:]+):', expand=False)
    df['message'] = df['full_message'].str.extract(message_pattern, expand=False)

    # For rows where sender extraction failed, assume the entire content after timestamp is the message
    #mask = df['sender'].isna()
    #df.loc[mask, 'message'] = df.loc[mask, 'full_message'].str.extract(r'\] (.+)$', expand=False)

    df['date_time'] = df['date_time'].apply(parse_date)
    df = df.drop(columns=['full_message'])

    return df


'''
## Compare and merge
- compare the two for any differences and merge them
'''


def compare_and_merge(df1, df2):
    pass


'''
## LLM transformations
- translate to english
- merge multiple messages into one (if they are from the same person) and the previous sentence seems incomplete
- rewrite the messages
'''


def translate_to_english(df):
    pass


def merge_messages(df):
    pass


def rewrite_messages(df):
    pass
