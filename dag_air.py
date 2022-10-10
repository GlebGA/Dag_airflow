#DAG в airflow, который будет считаться каждый день за вчера. 
#Параллельно будем обрабатывать две таблицы. В feed_actions для каждого юзера посчитаем число просмотров и лайков контента. В message_actions для каждого юзера считаем, сколько он получает и отсылает сообщений, скольким людям он пишет, сколько людей пишут ему. Каждая выгрузка должна быть в отдельном таске.
#Далее объединяем две таблицы в одну.
#Для этой таблицы считаем все эти метрики в разрезе по полу, возрасту и ос. Делаем три разных таска на каждый срез.
#И финальные данные со всеми метриками записываем в отдельную таблицу в ClickHouse.
#Каждый день таблица должна дополняться новыми данными. 

import pandas as pd
import pandahouse as ph
import numpy as np

from airflow.decorators import dag, task
from datetime import datetime, timedelta

connection = {
    'host': '#',
    'database':'#',
    'user':'#', 
    'password':'#'}

default_args = {
    'owner': 'g-genba-23',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 9, 20)}

schedule_interval = '0 10 * * *'

def age_segmentation(x): 
    
    if x < 25:
        segment = '< 25'
    
    elif 25 <= x < 46:
        segment = '25 - 45'
        
    else:
        segment = '46+'
        
    return segment


@dag(default_args=default_args,
     schedule_interval=schedule_interval,
     catchup=False)
def dag_g_genba():
    
    @task()
    def extract_feed():
        query = '''
                SELECT
                    user_id,
                    toDate(time) as event_date,
                    age,
                    gender,
                    os, 
                    countIf(action = 'like') as likes,
                    countIf(action = 'view') as views
                FROM
                    simulator_20220620.feed_actions
                WHERE
                    toDate(time) = today() - 1
                GROUP BY
                    user_id,
                    event_date,
                    age,
                    gender,
                    os
                '''
        df_feed = ph.read_clickhouse(query=query, connection=connection)

        return df_feed

    @task()
    def extract_messages():
        query = '''
                SELECT
                     *
                FROM
                    (SELECT
                        user_id,
                        gender,
                        age,
                        os,
                        toDate(time) as event_date,
                        COUNT(reciever_id) as messages_sent,
                        COUNT(DISTINCT reciever_id) as users_sent
                    FROM
                        simulator_20220620.message_actions
                    WHERE
                        toDate(time) = today() - 1
                    GROUP BY
                        user_id,
                        gender,
                        age,
                        os,
                        event_date) as l  

                    FULL OUTER JOIN

                        (SELECT
                            reciever_id as user_id_,
                            COUNT(user_id) as messages_received,
                            COUNT(DISTINCT user_id) as users_received
                        FROM
                            simulator_20220620.message_actions
                        WHERE
                            toDate(time) = today() - 1
                        GROUP BY
                            user_id_) as r
                        ON l.user_id = r.user_id_
                    '''
        df_messages = ph.read_clickhouse(query=query, connection=connection)

        return df_messages

    @task()
    def join_tables(df_feed, df_messages):
        df = df_feed.merge(df_messages, 
                           on = ['user_id', 'gender', 'age', 'os', 'event_date'], 
                           how = 'outer')\
                    .drop('user_id_', axis=1)

        df = df.loc[df['user_id'] != 0].fillna(0)

        return df

    @task()
    def groupby_gender(df):
        df_gender = df.groupby(['event_date', 'gender'], as_index = False)['likes', 'views', 
                                                                           'messages_sent','users_sent',
                                                                           'messages_received', 'users_received'].sum()
        df_gender['segment_type'] = 'gender'

        return df_gender.rename({'gender':'segment'}, axis=1)

    @task()
    def groupby_os(df):
        df_os = df.groupby(['event_date', 'os'], as_index = False)['likes', 'views', 
                                                                   'messages_sent', 'users_sent', 
                                                                   'messages_received', 'users_received'].sum()
        df_os['segment_type'] = 'os'

        return df_os.rename({'os':'segment'}, axis=1)

    @task()
    def groupby_age(df):

        df['age'] = df['age'].apply(age_segmentation)

        df_age = df.groupby(['event_date', 'age'], as_index = False)['likes', 'views', 
                                                                     'messages_sent', 'users_sent', 
                                                                     'messages_received', 'users_received'].sum()
        df_age['segment_type'] = 'age'

        return df_age.rename({'age':'segment'}, axis=1)

    @task()
    def load_tables(df_gender, df_os, df_age):

        final_df = pd.concat([df_gender, df_os, df_age], axis=0)
        final_df = final_df[['event_date', 'segment_type', 'segment', 
                             'likes', 'views', 'messages_sent', 'users_sent', 
                             'messages_received', 'users_received']]

        col_list = final_df.columns[3:9].to_list()

        for el in col_list:
            final_df[el] = final_df[el].astype('int')

        connection_test = {'host': '#', 
                           'password': '#',
                           'user': '#',
                           'database': '#'}

        ph.to_clickhouse(df = final_df,
                             table='g_genba_23', 
                             index=False,
                             connection=connection_test)

    df_feed = extract_feed()
    df_messages = extract_messages()
    df = join_tables(df_feed, df_messages)
    df_gender = groupby_gender(df)
    df_os = groupby_os(df)
    df_age = groupby_age(df)
    load_tables(df_gender, df_os, df_age)


dag_g_genba = dag_g_genba()