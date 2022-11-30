try:

    from datetime import timedelta
    from airflow import DAG
    from airflow.operators.python_operator import PythonOperator
    from datetime import datetime
    import pandas as pd

    print("All Dag modules are ok ......")
except Exception as e:
    print("Error  {} ".format(e))


def first_function_execute(**context):
    # print("first_function_execute   ")

    import firebase_admin
    from firebase_admin import credentials
    from firebase_admin import firestore

    # Use a service account.
    cred = credentials.Certificate('dags/firebase.json')

    app = firebase_admin.initialize_app(cred)

    db = firestore.client()


    API_KEY="1L0DfzlGY6SW0GSOUpaRMvtMU"
    API_SECRET="PP0P60ZHJuaHCtJBAZdF0iEdf6veMTa3DAeIRU3iCTeFw9Kwzc"
    BEARER_TOKEN="AAAAAAAAAAAAAAAAAAAAAEeoigEAAAAAgTTh1Vj3gsFgwMRYr9c1o%2F1tB%2FQ%3D4HB5HRxSZCJLQvgTn2zwXqPx2nwqAHFpNb5GNZITYLamCzrTfg"
    ACCESS_TOKEN="4406676328-ZxCKdMXP2lQCiSP8aUGRJQW2CuzyzkaQpzYv3Up"
    ACCESS_TOKEN_SECRET="7eIeS1lDUHtJNbqKuOE14unO4DwjTLpsSVaYODpVQyN7m"


    with open('dags/abusive_words_3.txt', 'r') as file:
        abusive_words = file.read().replace('\n', ' OR ')


    import tweepy

    doc_collection = db.collection(u'tweets')


    client = tweepy.Client(bearer_token=BEARER_TOKEN)
    response = client.search_recent_tweets(query=abusive_words, max_results=100, tweet_fields=['created_at','lang','author_id','public_metrics'],user_fields=['profile_image_url'], expansions=['author_id','attachments.media_keys'])
    users = {u['id']: u for u in response.includes['users']}


    tweetArr = []
    for tweet in response.data:
        if users[tweet.author_id]:
            user = users[tweet.author_id]
            obj = {
                "userInfo": {
                    "name": user.name,
                    "userName": user.username,
                    "userId": tweet.author_id,
                    "profileImage": user.profile_image_url
                },
                "tweetId": tweet.id,
                "attachments": tweet.attachments,
                "text": tweet.text,
                "created_at": tweet.created_at,
                "lang": tweet.lang,
                "metrics": tweet.public_metrics,
                "total_count": sum(tweet.public_metrics.values())
            }
            doc_ref = doc_collection.document(str(tweet.id))
            doc_ref.set(obj)

    context['ti'].xcom_push(key='mykey', value="first_function_execute says Hello ")


# def second_function_execute(**context):
#     instance = context.get("ti").xcom_pull(key="mykey")
#     data = [{"name":"Soumil","title":"Full Stack Software Engineer"}, { "name":"Nitin","title":"Full Stack Software Engineer"},]
#     df = pd.DataFrame(data=data)
#     print('@'*66)
#     print(df.head())
#     print('@'*66)

#     print("I am in second_function_execute got value :{} from Function 1  ".format(instance))


with DAG(
        dag_id="first_dag",
        schedule_interval="@daily",
        default_args={
            "owner": "airflow",
            "retries": 1,
            "retry_delay": timedelta(minutes=5),
            "start_date": datetime(2022, 11, 29),
        },
        catchup=False) as f:

    first_function_execute = PythonOperator(
        task_id="first_function_execute",
        python_callable=first_function_execute,
        provide_context=True,
        op_kwargs={"name":"Soumil Shah"}
    )

    # second_function_execute = PythonOperator(
    #     task_id="second_function_execute",
    #     python_callable=second_function_execute,
    #     provide_context=True,
    # )

first_function_execute 
# >> second_function_execute
